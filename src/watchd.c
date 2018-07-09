#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <stdarg.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/process_ctrl.h"
#include "fastcommon/ini_file_reader.h"

#define MAX_CRON_PROCESS_PER_ENTRY 64
#define DEFAULT_WAIT_SUBPROCESS 300
#define DEFAULT_RESTART_INTERVAL 1000
#define DEFAULT_CHECK_ALIVE_INTERVAL 0
#define MAX_NAME_SIZE    64
#define MAX_PARAM_COUNT  64

#define MODE_FAILOVER 'f'
#define MODE_ALL      'a'

static char base_path[MAX_PATH_SIZE]={0};
static const char* action = "start";
static const char* configfile = NULL;
static char run_by_user[MAX_NAME_SIZE]={0};
static char run_by_group[MAX_NAME_SIZE]={0};
static char service_name[MAX_NAME_SIZE]={0};
static const char* program = NULL;
static int subprocess_number = 1;
static int wait_subprocess_ms = DEFAULT_WAIT_SUBPROCESS;
static int restart_interval_ms = DEFAULT_RESTART_INTERVAL;
static int check_alive_interval = DEFAULT_CHECK_ALIVE_INTERVAL;
static time_t last_check_alive_time;
static bool enable_access_log = false;
static pthread_t schedule_tid;
static bool continue_flag = true;
static volatile bool restart_subprocess = false;
static int log_file_keep_days = 0;

static char pidfile[MAX_PATH_SIZE];
static char logfile[MAX_PATH_SIZE];
static char logpath[MAX_PATH_SIZE];

static int parse_args(int argc, char* argv[]);
static int setup_sig_handlers();
static int setup_schedule_tasks();
static int make_dir(const char* dirname);
static int load_from_conf_file(const char* filename);

typedef enum { hc_type_none=0, hc_type_kill,
    hc_type_exec, hc_type_library } HealthCheckType;

typedef int (*health_check_func)(int argc, char **argv);

typedef struct command_params {
    bool run_by_sh;
    char *cmd;    //command line
    int argc;
    char **argv;
} CommandParams;

typedef struct command_entry {
    CommandParams command;  //exec command
    struct health_check_entry {
        CommandParams command;
        HealthCheckType type;
        health_check_func func;
    } health_check;
} CommandEntry;

typedef struct child_process_info {
    int pid;
    bool running;
    char mode;  //run mode
    bool enable_access_log;
    int64_t last_start_time_ms;
    int last_check_alive_time;
    int restart_interval_ms;
    int check_alive_interval;
    uint32_t run_count;
    char *logfile;
    char *acclog;
    struct command_array {
        int alloc;   //alloc count
        int count;   //item count
        int index;   //current index
        CommandEntry *list;
    } commands;
} ChildProcessInfo;

typedef struct cron_entry {
    TimeInfo time_base;
    int interval;
} CronEntry;

typedef struct child_process_array {
    ChildProcessInfo **processes;
    int alloc_size;
    int count;
} ChildProcessArray;

static struct fast_mblock_man process_mblock;
static ChildProcessArray child_proc_array = {NULL, 0, 0};
static int child_running = 0;

static ChildProcessArray cron_proc_array = {NULL, 0, 0};

typedef char MqPath[MAX_PATH_SIZE];
static MqPath* logfiles_all = NULL;
static MqPath* acclogs_all = NULL;

static int logfiles_count = 0;

static IniContext ini_context;
IniContext *iniContext = &ini_context;
static int cron_entry_alloc_size = 0;
static int cron_entry_count = 0;
static CronEntry *cron_entries = NULL;

typedef ChildProcessInfo* (*malloc_process_func)();

static int expand_cmd(ChildProcessInfo *cpro, malloc_process_func malloc_func,
        ChildProcessInfo **processes, int *pnum, const int max_count);
static int set_command_params(CommandParams *command, const bool enable_access_log,
        char *acclog);
static int process_set_command_params(ChildProcessInfo* cpro);

static void usage(const char* program)
{
    printf("usage: %s <config file> [start|stop|restart]\n", program);
}

static int update_process(int pid, const int status);
static int check_all_processes();
static int start_all_processes();
static int stop_all_processes();
static int rotate_logs();
static void check_subproccess_alive();
static int start_process(ChildProcessInfo *process);
static int add_shedule_entries();

int main(int argc, char* argv[])
{
    int result;
    bool stop = false;

    if ((result=parse_args(argc, argv)) != 0) {
        return result;
    }

    g_current_time = time(NULL);
    log_init2();
    log_set_fd_flags(&g_log_context, O_CLOEXEC);
    log_set_rotate_time_format(&g_log_context, "%Y%m%d");
 
    if ((result=fast_mblock_init_ex(&process_mblock,
                    sizeof(ChildProcessInfo), 256, NULL, false)) != 0)
    {
        return result;
    }

    result = load_from_conf_file(configfile);
    if (result) {
        logCrit("file: "__FILE__", line: %d, "
                "load from conf file fail, "
                "errno: %d, error info: %s",
                __LINE__, result, strerror(result));
        return result;
    }

    log_set_keep_days(&g_log_context, log_file_keep_days);

    umask(0);
    if ((result = make_dir(logpath)) != 0) {
        logError("file: "__FILE__", line: %d, mkdir %s fail, "
                "errno: %d, error info: %s", __LINE__,
                logpath, result, strerror(result));
        return result;
    }

    if ((result=process_action(pidfile, action, &stop)) != 0) {
        if (result == EINVAL) {
            usage(argv[0]);
        }
        log_destroy();
        return result;
    }
    if (stop) {
        log_destroy();
        return 0;
    }

    if ((result=set_run_by(run_by_group, run_by_user)) != 0) {
        logCrit("file: "__FILE__", line: %d, "
            "call set set_run_by fail, exit!", __LINE__);
        return result;
    }

    daemon_init(false);
    umask(0);

    log_set_use_file_write_lock(true);
    if ((result=log_set_filename(logfile)) != 0) {
        if (result == EAGAIN || result == EACCES) {
            logCrit("file: "__FILE__", line: %d, "
                    "the process already running, "
                    "please kill the old process first!", __LINE__);
        } else {
            logCrit("file: "__FILE__", line: %d, "
                    "call set log_set_filename fail, exit!", __LINE__);
        }
        return result;
    }
    close(0);

    if ((result=write_to_pid_file(pidfile)) != 0) {
        log_destroy();
        return result;
    }
    setup_sig_handlers();
    setup_schedule_tasks();

    if ((result=add_shedule_entries()) != 0) {
        return result;
    }
    if ((result = start_all_processes()) != 0) {
        return result;
    }

    iniFreeContext(iniContext);
    last_check_alive_time = g_current_time;
    logInfo("file: "__FILE__", line: %d, %s started, "
            "running processes count: %d",
            __LINE__, program, child_running);

    sched_print_all_entries();

    while (continue_flag) {
        if (restart_subprocess) {
            restart_subprocess = false;
            stop_all_processes();
        }
        if ((result = check_all_processes()) != 0) {
            return result;
        }
        check_subproccess_alive();

        if (child_running < child_proc_array.count &&
                (result = start_all_processes()) != 0)
        {
            return result;
        }
        usleep(10*1000);
    }

    stop_all_processes();
    delete_pid_file(pidfile);
    logInfo("file: "__FILE__", line: %d, %s exited normally",
            __LINE__, program);
    log_destroy();
    return 0;
}

static void sigChildHandler(int sig)
{
}

static void sigQuitHandler(int sig)
{
    continue_flag = false;
}

static int setup_sig_handlers()
{
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    sigemptyset(&act.sa_mask);

    act.sa_handler = sigChildHandler;
    if(sigaction(SIGCHLD, &act, NULL) < 0) {
        logCrit("file: "__FILE__", line: %d, "
            "call sigaction fail, errno: %d, error info: %s",
            __LINE__, errno, strerror(errno));
        return errno;
    }

    act.sa_handler = SIG_IGN;
    if(sigaction(SIGPIPE, &act, NULL) < 0 ||
        sigaction(SIGHUP, &act, NULL) < 0)
    {
        logCrit("file: "__FILE__", line: %d, "
            "call sigaction fail, errno: %d, error info: %s",
            __LINE__, errno, strerror(errno));
        return errno;
    }

    act.sa_handler = sigQuitHandler;
    if(sigaction(SIGINT, &act, NULL) < 0 ||
        sigaction(SIGTERM, &act, NULL) < 0 ||
        sigaction(SIGQUIT, &act, NULL) < 0)
    {
        logCrit("file: "__FILE__", line: %d, "
            "call sigaction fail, errno: %d, error info: %s",
            __LINE__, errno, strerror(errno));
        return errno;
    }

    return 0;
}

static int parse_args(int argc, char* argv[])
{
    int len;
    
    for (len = strlen(argv[0]) - 1; len >= 0; len --) {
        if (argv[0][len] == '/') {
            break;
        }
    }
    program = argv[0] + len + 1;

    if (argc >= 2) {
        configfile = argv[1];
    } else {
        usage(argv[0]);
        return 1;
    }
    action = "start";
    if (argc >= 3) {
        action = argv[2];
    }

    if (strcmp(action, "start") == 0 || strcmp(action, "stop") == 0
        || strcmp(action, "restart") == 0)
    {
        return 0;
    }
    usage(argv[0]);
    return 1;
}

static int make_dir(const char* dirname)
{
    char tpath[MAX_PATH_SIZE];
    int i = 0, r = 0;

    snprintf(tpath, sizeof(tpath), "%s/", dirname);
    for (i = 1; tpath[i] != '\0'; i++) {
        if (tpath[i] == '/') {
            tpath[i] = '\0';
            r = mkdir(tpath, 0777);
            if (r != 0 && errno != EEXIST) {
                logError("file: "__FILE__", line: %d, "
                        "mkdir %s fail, errno: %d, error info: %s",
                        __LINE__, tpath, errno, strerror(errno));
                return errno;
            }
            tpath[i] = '/';
        }
    }
    return 0;
}

static int check_alloc_command_array(struct command_array *commands,
        const int inc_count)
{
     CommandEntry *list;
     int bytes;
     int alloc_size;
     if (commands->alloc > commands->count + inc_count) {
         return 0;
     }

     alloc_size = commands->alloc == 0 ? 4 : commands->alloc * 2;
     while (alloc_size < commands->count + inc_count) {
         alloc_size *= 2;
     }

     bytes = sizeof(CommandEntry) * alloc_size;
     list = (CommandEntry *)malloc(bytes);
     if (list == NULL) {
         logError("file: "__FILE__", line: %d, malloc %d bytes fail",
                 __LINE__, bytes);
         return ENOMEM;
     }

     memset(list, 0, bytes);
     if (commands->count > 0) {
         memcpy(list, commands->list, sizeof(CommandEntry) * commands->count);
     }

     if (commands->list != NULL) {
         free(commands->list);
     }
     commands->alloc = alloc_size;
     commands->list = list;
     return 0;
}

static inline CommandEntry *get_current_command_entry(ChildProcessInfo* proc)
{
    return proc->commands.list + proc->commands.index;
}

static inline char *get_current_command(ChildProcessInfo* proc)
{
    return proc->commands.list[proc->commands.index].command.cmd;
}

static inline CommandParams *get_next_command(ChildProcessInfo* proc)
{
    if (proc->commands.count > 1 && proc->run_count > 0) {
        proc->commands.index++;
        if (proc->commands.index >= proc->commands.count) {
            proc->commands.index = 0;
        }
    }

    proc->run_count++;
    return &proc->commands.list[proc->commands.index].command;
}

static int process_info_cmp_pid(const void *p1, const void *p2)
{
    return (*((ChildProcessInfo **)p1))->pid - (*((ChildProcessInfo **)p2))->pid;
}

static int schedule_task_func(void *args)
{
    ChildProcessInfo *process;
    process = (ChildProcessInfo *)args;

    logInfo("file: "__FILE__", line: %d, run cron process%s: %s %s",
            __LINE__, get_current_command_entry(process)->command.run_by_sh ?
            "(run by sh -c)" : "", get_current_command(process),
            process->enable_access_log ? process->acclog : "");
    if (start_process(process) == 0) {
        if (cron_proc_array.count > 1) {
            qsort(cron_proc_array.processes, cron_proc_array.count,
                    sizeof(ChildProcessInfo *), process_info_cmp_pid);
        }
    }
    return 0;
}

static ChildProcessInfo *malloc_process_entry(ChildProcessArray *processArray)
{
    int bytes;
    ChildProcessInfo *process;

    if (processArray->count >= processArray->alloc_size) {
        if (processArray->alloc_size == 0) {
            processArray->alloc_size = 32;
        } else {
            processArray->alloc_size *= 2;
        }
        bytes = sizeof(ChildProcessInfo *) * processArray->alloc_size;
        processArray->processes = (ChildProcessInfo **)realloc(
                processArray->processes, bytes);
        if (processArray->processes == NULL) {
            logError("file: "__FILE__", line: %d, malloc %d bytes fail",
                    __LINE__, bytes);
            return NULL;
        }
        memset(processArray->processes + processArray->count, 0,
                sizeof(ChildProcessInfo *) * (processArray->alloc_size -
                    processArray->count));
    }

    process = (ChildProcessInfo *)fast_mblock_alloc_object(&process_mblock);
    if (process != NULL) {
        memset(process, 0, sizeof(ChildProcessInfo));
        processArray->processes[processArray->count++] = process;
    }
    return process;
}

static ChildProcessInfo *malloc_child_process_entry()
{
    return malloc_process_entry(&child_proc_array);
}

static ChildProcessInfo *malloc_cron_process_entry()
{
    return malloc_process_entry(&cron_proc_array);
}

static int check_alloc_schedule_entries(ScheduleArray *pSheduleArray,
        int *alloc_size, const int inc)
{
    int bytes;
    if (pSheduleArray->count + inc > *alloc_size) {
        if (*alloc_size == 0) {
            *alloc_size = 64;
        }
        else {
            *alloc_size *= 2;
        }
        while (pSheduleArray->count + inc > *alloc_size) {
            *alloc_size *= 2;
        }

        bytes = sizeof(ScheduleEntry) * (*alloc_size);
        pSheduleArray->entries = (ScheduleEntry *)realloc(pSheduleArray->entries, bytes);
        if (pSheduleArray->entries == NULL) {
            logError("file: "__FILE__", line: %d, malloc %d bytes fail",
                    __LINE__, bytes);
            return ENOMEM;
        }
    }

    return 0;
}

static int add_shedule_entries()
{
    ChildProcessInfo *cron_processes[MAX_CRON_PROCESS_PER_ENTRY];
    ChildProcessInfo *process;
    CronEntry *pCronEntry;
    ScheduleEntry *pScheduleEntry;
    ScheduleArray shedule_array = {NULL, 0};
    int alloc_size = 0;
    int i, k;
    int count;
    int result;

    if (cron_entry_count == 0) {
        return 0;
    }

    for (i=0; i<cron_entry_count; i++) {
        pCronEntry = cron_entries + i;
        count = 0;
        result = expand_cmd(cron_proc_array.processes[i],
                malloc_cron_process_entry, cron_processes, &count,
                MAX_CRON_PROCESS_PER_ENTRY);
        if (result != 0) {
            return result;
        }
        if ((result=check_alloc_schedule_entries(&shedule_array,
                        &alloc_size, count)) != 0)
        {
            return result;
        }

        for (k=0; k<count; k++) {
            process = cron_processes[k];
            pScheduleEntry = shedule_array.entries + shedule_array.count;
            INIT_SCHEDULE_ENTRY_EX((*pScheduleEntry), sched_generate_next_id(),
                    pCronEntry->time_base, pCronEntry->interval,
                    schedule_task_func, process);
            shedule_array.count++;
        }
    }

    logInfo("cron_proc_array.count: %d", cron_proc_array.count);
    for (i = 0; i < cron_proc_array.count; i++) {
        if ((result=process_set_command_params(cron_proc_array.processes[i])) != 0) {
            return result;
        }
    }

    if ((result=sched_add_entries(&shedule_array)) != 0) {
        return result;
    }

    free(shedule_array.entries);
    free(cron_entries);
    cron_entries = NULL;
    cron_entry_count = 0;
    cron_entry_alloc_size = 0;
    return 0;
}

static int add_cron_entry(ChildProcessInfo *process,
        const char *time_base, const int interval)
{
    int bytes;
    int result;
    CronEntry *pEntry;

    if (cron_entry_count >= cron_entry_alloc_size) {
        if (cron_entry_alloc_size == 0) {
            cron_entry_alloc_size = 64;
        }
        else {
            cron_entry_alloc_size *= 2;
        }
        bytes = sizeof(CronEntry) * cron_entry_alloc_size;
        cron_entries = (CronEntry *)realloc(cron_entries, bytes);
        if (cron_entries == NULL) {
            logError("file: "__FILE__", line: %d, malloc %d bytes fail",
                    __LINE__, bytes);
            return ENOMEM;
        }
    }

    pEntry = cron_entries + cron_entry_count;
    pEntry->interval = interval;
    result = get_time_item_from_str(time_base, "time_base", &pEntry->time_base, 0, 0);
    if (result != 0) {
        return result;
    }
    cron_entry_count++;
    return 0;
}

static inline bool is_run_by_sh(const char *cmd)
{
    return (strchr(cmd, '>') != NULL || cmd[strlen(cmd) - 1] == '&');
}

static int ini_section_load(const int index, const HashData *data, void *args)
{
    IniSection *pSection;
    IniItem *pItem;
    IniItem *pItemEnd;
    char section_name[256];
    int section_len;
    int i;

    pSection = (IniSection *)data->value;
    if (pSection == NULL) {
        return 0;
    }

    section_len = data->key_len;
    if (section_len >= sizeof(section_name)) {
        section_len = sizeof(section_name) - 1;
    }

    memcpy(section_name, data->key, section_len);
    *(section_name + section_len) = '\0';

    if (pSection->count > 0) {
        const char* cmd = NULL;
        char *type;
        char *mode;
        char *check_alive_command;
        char *time_base;
        int cnum = subprocess_number;
        int new_restart_interval_ms = restart_interval_ms;
        int new_check_alive_interval = check_alive_interval;
        int repeat_interval;
        bool enableAccessLog = enable_access_log;


        type = NULL;
        mode = NULL;
        check_alive_command = NULL;
        time_base = NULL;
        repeat_interval = 86400;
        pItemEnd = pSection->items + pSection->count;
        for (pItem=pSection->items; pItem<pItemEnd; pItem++) {
            if (strcmp(pItem->name, "subprocess_command") == 0) {
                cmd = pItem->value;
            } else if (strcmp(pItem->name, "subprocess_number") == 0) {
                cnum = atoi(pItem->value);
            } else if (strcmp(pItem->name, "restart_interval_ms") == 0) {
                new_restart_interval_ms = atoi(pItem->value);
            } else if (strcmp(pItem->name, "check_alive_interval") == 0) {
                new_check_alive_interval = atoi(pItem->value);
            } else if (strcmp(pItem->name, "check_alive_command") == 0) {
                check_alive_command = pItem->value;
            } else if (strcmp(pItem->name, "type") == 0) {
                type = pItem->value;
            } else if (strcmp(pItem->name, "mode") == 0) {
                mode = pItem->value;
            } else if (strcmp(pItem->name, "time_base") == 0) {
                time_base = pItem->value;
            } else if (strcmp(pItem->name, "repeat_interval") == 0) {
                repeat_interval = atoi(pItem->value);
                if (repeat_interval <= 0) {
                    repeat_interval = 86400;
                    logWarning("file: "__FILE__", line: %d, "
                            "invalid repeat_interval for section %s, "
                            "set to %d", __LINE__,
                            section_name, repeat_interval);
                }
            } else if (strcmp(pItem->name, "enable_access_log") == 0) {
                enableAccessLog = FAST_INI_STRING_IS_TRUE(pItem->value);
            }
        }

        if (cmd == NULL || *cmd == '\0') {
            logError("file: "__FILE__", line: %d, section %s, "
                    "expect subprocess_command", __LINE__, section_name);
            return EINVAL;
        }

        snprintf(logfiles_all[logfiles_count], MAX_PATH_SIZE,
                "%s/%s-%s.log", logpath, service_name, section_name);
        if (enableAccessLog) {
            snprintf(acclogs_all[logfiles_count], MAX_PATH_SIZE,
                    "%s/%s-%s-access.log", logpath, service_name, section_name);
        } else {
            *acclogs_all[logfiles_count] = '\0';
        }
        if (type != NULL && strlen(type) >= 4 && strncmp(type, "cron", 4) == 0) {
            ChildProcessInfo* cpro;
            cpro = malloc_cron_process_entry();
            if (cpro == NULL) {
                return ENOMEM;
            }
            if (check_alloc_command_array(&cpro->commands, 1) != 0) {
                return ENOMEM;
            }
            cpro->commands.list[0].command.run_by_sh = is_run_by_sh(cmd);
            cpro->commands.list[0].command.cmd = strdup(cmd);
            cpro->commands.count = 1;
            cpro->logfile = strdup(logfiles_all[logfiles_count]);
            cpro->acclog = strdup(acclogs_all[logfiles_count]);
            cpro->mode = MODE_ALL;
            cpro->enable_access_log = enableAccessLog;
            return add_cron_entry(cpro, time_base, repeat_interval);
        }

        if (cnum > 0 && new_restart_interval_ms >= 0) {
            for (i = 0; i < cnum; i ++) {
                ChildProcessInfo* cpro;
                cpro = malloc_child_process_entry();
                if (cpro == NULL) {
                    return ENOMEM;
                }
                cpro->pid = 0;
                if (check_alloc_command_array(&cpro->commands, 1) != 0) {
                    return ENOMEM;
                }
                if (mode != NULL && strcmp(mode, "failover") == 0) {
                    cpro->mode = MODE_FAILOVER;
                } else {
                    cpro->mode = MODE_ALL;
                }
                cpro->commands.list[0].command.run_by_sh = is_run_by_sh(cmd);
                cpro->commands.list[0].command.cmd = strdup(cmd);

                if (new_check_alive_interval > 0) {
                    if (check_alive_command != NULL) {
                        cpro->commands.list[0].health_check.command.cmd = strdup(check_alive_command);
                    }
                }

                cpro->commands.count = 1;
                cpro->logfile = logfiles_all[logfiles_count];
                cpro->acclog = acclogs_all[logfiles_count];
                cpro->restart_interval_ms = new_restart_interval_ms;
                cpro->check_alive_interval = new_check_alive_interval;
                cpro->enable_access_log = enableAccessLog;
            }
            logfiles_count++;
        } else {
            logError("file: "__FILE__", line: %d, invalid config "
                    "for section %s subprocess_command: %s"
                    " subprocess_number: %d restart_interval_ms %d",
                    __LINE__, section_name, cmd, cnum, new_restart_interval_ms);
            return EINVAL;
        }
    }

    return 0;
}

static int expand_params(char *str, char *out_buff, const int buff_size,
        char **params, const int max_count)
{
    char *pStart;
    char *pMid;
    char *p;
    int start;
    int end;
    int len;
    int count;
    int i;

    pStart = str + 1;
    pMid = strchr(pStart, '-');
    if (pMid == NULL) {
        return 0;
    }

    while (*pStart == ' ' || *pStart == '\t') {
        pStart++;
    }
    p = pStart;
    while (*p >= '0' && *p <= '9') {
        p++;
    }
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (p != pMid) {
        return 0;
    }
    start = atoi(pStart);

    pStart = pMid + 1;
    while (*pStart == ' ' || *pStart == '\t') {
        pStart++;
    }
    p = pStart;
    while (*p >= '0' && *p <= '9') {
        p++;
    }
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (*(p + 1) != '\0') {
        return 0;
    }
    end = atoi(pStart);

    if ((end - start) + 1 > max_count) {
        logError("file: "__FILE__", line: %d, item count: %d "
                "exceeds max: %d", __LINE__,
                end - start + 1, max_count);
        return 0;
    }

    count = 0;
    len = 0;
    for (i=start; i<=end; i++) {
        if (len + 16 > buff_size) {
            logError("file: "__FILE__", line: %d, expect buffer "
                    "size: %d exceed: %d", __LINE__, len + 16, buff_size);
            return 0;
        }

        p = out_buff + len;
        params[count++] = p;
        len += sprintf(p, "%d", i) + 1;
    }
    return count;
}

static int get_params(char *str, char *out_buff, const int buff_size,
        char **params, const int max_count)
{
    int count;
    if (*str == '['  && *(str + strlen(str) - 1) == ']') {
        count = expand_params(str, out_buff, buff_size,
                params, max_count);
        if (count > 0) {
            return count;
        }
    }

    return splitEx(str, ',', params, max_count);
}

static int expand_cmd(ChildProcessInfo *cpro,
        malloc_process_func malloc_func, ChildProcessInfo **processes,
        int *pnum, const int max_count)
{
#define MAX_PARAMS_COUNT 256

    char *cmd;
    char *new_cmd;
    char *pdollar;
    char pword[64];
    char *pworde;
    char *confArgs;
    char *tail;
    char args[MAX_PATH_SIZE];
    char *params[MAX_PARAMS_COUNT];
    char out_buff[1024];
    int i;
    int count;
    int cmd_len;
    int word_len;
    int front_len;

    cmd = cpro->commands.list[0].command.cmd;
    cmd_len = strlen(cmd);
    pdollar = (char*)strchr(cmd, '$');
    if (pdollar == NULL) { //no need to expand
        if (processes != NULL) {
            processes[0] = cpro;
            *pnum = 1;
        }
        return 0;
    }
    pworde = pdollar + 1;
    while (*pworde != '\0' && !isspace(*pworde)) {
        pworde++;
    }

    tail = pworde;
    word_len = pworde - (pdollar + 1);
    if (word_len >= sizeof(pword)) {
        logError("file: "__FILE__", line: %d, key length "
                "too long, exceeds %d, key: %.*s. in cmd: %s",
                __LINE__, (int)sizeof(pword), word_len,
                pdollar + 1, cmd);
        return EINVAL;
    }

    sprintf(pword, "%.*s", word_len, pdollar + 1);
    confArgs = iniGetStrValue(NULL, pword, iniContext);
    if (confArgs == NULL) {
        logError("file: "__FILE__", line: %d, no conf word for "
                "%s in global section. in cmd: %s",
                __LINE__, pword, cmd);
        return EINVAL;
    }
    if ((int)strlen(confArgs) >= (int)sizeof(args)) {
        logError("file: "__FILE__", line: %d, the value of "
                "%s in global section is too long",
                __LINE__, pword);
        return ENOSPC;
    }

    strcpy(args, confArgs);
    count = get_params(args, out_buff, sizeof(out_buff),
            params, MAX_PARAMS_COUNT);

    front_len = pdollar - cmd;
    if (cpro->mode == MODE_ALL) {
        for (i=1; i<count; i++) {
            ChildProcessInfo* lpro;
            lpro = malloc_func();
            if (lpro == NULL) {
                return ENOSPC;
            }

            memcpy(lpro, cpro, sizeof *cpro);
            memset(&lpro->commands, 0, sizeof(lpro->commands));
            if (check_alloc_command_array(&lpro->commands, 1) != 0) {
                return ENOMEM;
            }
            lpro->commands.list[0].command.cmd = malloc(cmd_len + strlen(params[i]) + 1);
            if (lpro->commands.list[0].command.cmd == NULL) {
                logError("file: "__FILE__", line: %d, malloc %d bytes fail",
                        __LINE__, (int)(cmd_len + strlen(params[i])) + 1);
                return ENOMEM;
            }
            lpro->commands.count = 1;
            memcpy(lpro->commands.list[0].command.cmd, cmd, front_len);
            sprintf(lpro->commands.list[0].command.cmd + front_len, "%s%s", params[i], tail);

            if (processes != NULL) {
                if (i < max_count) {
                    processes[i] = lpro;
                } else {
                    logWarning("file: "__FILE__", line: %d, "
                            "exceeds max count: %d",
                            __LINE__, max_count);
                }
            }
        }

        if (processes != NULL) {
            processes[0] = cpro;
            *pnum = count;
        }
    } else {  //failover
        if (check_alloc_command_array(&cpro->commands, count) != 0) {
            return ENOMEM;
        }
        for (i=1; i<count; i++) {
            cpro->commands.list[i].command.run_by_sh = cpro->commands.list[0].command.run_by_sh;
            cpro->commands.list[i].command.cmd = malloc(cmd_len + strlen(params[i]) + 1);
            if (cpro->commands.list[i].command.cmd == NULL) {
                logError("file: "__FILE__", line: %d, malloc %d bytes fail",
                        __LINE__, (int)(cmd_len + strlen(params[i])) + 1);
                return ENOMEM;
            }
            memcpy(cpro->commands.list[i].command.cmd, cmd, front_len);
            sprintf(cpro->commands.list[i].command.cmd + front_len, "%s%s", params[i], tail);
            cpro->commands.list[i].health_check.command.cmd = cpro->commands.list[0].health_check.command.cmd;
        }
        cpro->commands.count = count;
        if (processes != NULL) {
            processes[0] = cpro;
            *pnum = 1;
        }
    }

    new_cmd = malloc(cmd_len + strlen(params[0]) + 1);
    if (new_cmd == NULL) {
        logError("file: "__FILE__", line: %d, malloc %d bytes fail",
                __LINE__, (int)(cmd_len + strlen(params[0])) + 1);
        return ENOMEM;
    }
    memcpy(new_cmd, cmd, front_len);
    sprintf(new_cmd + front_len, "%s%s", params[0], tail);
    free(cpro->commands.list[0].command.cmd);
    cpro->commands.list[0].command.cmd = new_cmd;
    return 0;
}

static int expand_child_cmd(ChildProcessInfo *cpro)
{
    return expand_cmd(cpro, malloc_child_process_entry, NULL, NULL, 0);
}

static char *get_command_param(char **str, char *end)
{
    char *p;
    char *start;
    char quote;

    p = *str;
    if (!(*p == '\'' || *p == '"')) {
        return p;
    }

    quote = *p;
    start = ++p;
    while (p < end && *p != quote) {
        p++;
    }
    if (p == end) {
        logError("file: "__FILE__", line: %d, "
                "expect quote char: %c!",
                __LINE__, quote);
        return NULL;
    }
    if (p + 1 < end && *(p + 1) != ' ') {
        logError("file: "__FILE__", line: %d, "
                "expect space char, but char %c occurs!",
                __LINE__, *(p + 1));
        return NULL;
    }

    *p = '\0';
    *str = p + 1;
    return start;
}

static int split_command_params(char *cmd, char **argv, int *argc,
        const int max_count)
{
    int count;
    char *p;
    char *end;

    count = 0;
    p = cmd;
    end = cmd + strlen(cmd);
    argv[count] = get_command_param(&p, end);
    if (argv[count] == NULL) {
        return EINVAL;
    }
    count++;

    while ((p = strchr(p, ' ')) != NULL) {
        *p++ = '\0';
        while (*p == ' ') p++;
        if (*p != '\0') {
            if (count < max_count) {
                argv[count] = get_command_param(&p, end);
                if (argv[count] == NULL) {
                    return EINVAL;
                }
                count++;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "too many parameters exceeds %d!",
                        __LINE__, max_count);
                return ENAMETOOLONG;
            }
        }
    }

    *argc = count;
    return 0;
}


static int set_command_params(CommandParams *command, const bool enable_access_log,
        char *acclog)
{
    char *argv[MAX_PARAM_COUNT + 2];
    char *cmd;
    int result;
    int argc;
    int bytes;

    argc = 0;
    if (command->run_by_sh) {
        argv[argc++] = strdup("/bin/sh");
        argv[argc++] = "-c";
        argv[argc++] = command->cmd;
    } else {
        cmd = strdup(command->cmd);
        result = split_command_params(cmd, argv, &argc, MAX_PARAM_COUNT);
        if (result != 0) {
            free(cmd);
            return result;
        }

        if (enable_access_log) {
            argv[argc++] = acclog;
        }
    }
    argv[argc++] = NULL;

    bytes = sizeof(char *) * argc;
    command->argv = (char **)malloc(bytes);
    if (command->argv == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    command->argc = argc;
    memcpy(command->argv, argv, bytes);
    return 0;
}

static int replace_check_alive_command(CommandEntry *entry)
{
    char *src;
    char *dest;
    char *start;
    char *p;
    char *end;
    char *new_cmd;
    int bytes;
    int num_len;
    int param_len;
    int n;
    char num[4];

    bytes = strlen(entry->command.cmd) + strlen(entry->health_check.command.cmd);
    new_cmd = (char *)malloc(bytes);
    if (new_cmd == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    dest = new_cmd;
    src = entry->health_check.command.cmd;
    end = entry->health_check.command.cmd + strlen(entry->health_check.command.cmd);
    while (src < end) {
        if (*src != '$') {
            *dest++ = *src++;
            continue;
        }

        start = p = src + 1;
        while (p < end && (*p >= '0' && *p <= '9')) {
            p++;
        }

        num_len = p - start;
        if (num_len == 0) {
            *dest++ = *src++;
            continue;
        }

        if (num_len >= (int)sizeof(num)) {
            logError("file: "__FILE__", line: %d, "
                    "group number %.*s is too large",
                    __LINE__, num_len, start);
            return ENAMETOOLONG;
        }
        memcpy(num, start, num_len);
        *(num + num_len) = '\0';
        n = atoi(num);
        if (n < 1 || n >= entry->command.argc) {
            logError("file: "__FILE__", line: %d, "
                    "group number %d is invalid",
                    __LINE__, n);
            return ENAMETOOLONG;
        }

        param_len = strlen(entry->command.argv[n]);
        memcpy(dest, entry->command.argv[n], param_len);
        dest += param_len;
        src = p;
    }

    *dest = '\0';
    free(entry->health_check.command.cmd);
    entry->health_check.command.cmd = new_cmd;
    return 0;
}

static int parse_check_alive_command(ChildProcessInfo* cpro)
{
    int i;
    int result;
    struct health_check_entry *health_check;

    for (i=0; i<cpro->commands.count; i++) {
        health_check = &cpro->commands.list[i].health_check;
        if (strchr(health_check->command.cmd, '$') != NULL) {
            result = replace_check_alive_command(cpro->commands.list + i);
            if (result != 0) {
                return result;
            }
        }

        result = set_command_params(&health_check->command, false, NULL);
        if (result != 0) {
            return result;
        }

        logInfo("cmd: %s", cpro->commands.list[i].command.cmd);
        logInfo("check cmd: %s", health_check->command.cmd);
    }

    return 0;
}

static int parse_check_alive_commands()
{
    int result;
    int i;
    for (i = 0; i < child_proc_array.count; i++) {
        ChildProcessInfo* child = child_proc_array.processes[i];
        if (child->check_alive_interval > 0) {
            result = parse_check_alive_command(child);
            if (result != 0) {
                return result;
            }
        }
    }

    return 0;
}

static int process_set_command_params(ChildProcessInfo* cpro)
{
    int i;
    int result;

    for (i=0; i<cpro->commands.count; i++) {
        result = set_command_params(&cpro->commands.list[i].command,
                cpro->enable_access_log, cpro->acclog);
        if (result != 0) {
            return result;
        }
    }

    return 0;
}

static int load_from_conf_file(const char* filename)
{
    int result;
    int i;
    const char* p;

    memset(iniContext, 0, sizeof(IniContext));
    result = iniLoadFromFileEx(filename, iniContext,
            FAST_INI_ANNOTATION_WITH_BUILTIN,
            NULL, 0, FAST_INI_FLAGS_SHELL_EXECUTE);
    if (result != 0) {
        logError("file: "__FILE__", line: %d, load conf file %s fail, "
                "ret code: %d", __LINE__, filename, result);
        return result;
    }
    p = iniGetStrValue(NULL, "run_by_group", iniContext);
    if (p) {
        strcpy(run_by_group, p);
    }
    p = iniGetStrValue(NULL, "run_by_user", iniContext);
    if (p) {
        strcpy(run_by_user, p);
    }
    load_log_level(iniContext);

    p = iniGetStrValue(NULL, "base_path", iniContext);
    if (p == NULL || p[0] == '\0') {
        logError("file: "__FILE__", line: %d, base_path should be set",
                __LINE__);
        return EINVAL;
    } else {
        strcpy(base_path, p);
    }
    p = iniGetStrValue(NULL, "service_name", iniContext);
    if (p == NULL || p[0] == '\0') {
        logError("file: "__FILE__", line: %d, "
                "service_name should be set in config", __LINE__);
        return EINVAL;
    } else {
        strcpy(service_name, p);
    }
    snprintf(pidfile, sizeof pidfile, "%s/watchd-%s.pid", base_path, service_name);
    snprintf(logpath, sizeof logpath, "%s/logs", base_path);
    snprintf(logfile, sizeof logfile, "%s/watchd-%s.log", logpath, service_name);

    log_file_keep_days = iniGetIntValue(NULL, "log_file_keep_days", iniContext, 0);
    if (log_file_keep_days < 0) {
        log_file_keep_days = 0;
    }

    subprocess_number = iniGetIntValue(NULL, "subprocess_number", iniContext, 1);
    if (subprocess_number <= 0) {
        subprocess_number = 1;
    }

    wait_subprocess_ms = iniGetIntValue(NULL, "wait_subprocess_ms",
            iniContext, DEFAULT_WAIT_SUBPROCESS);
    if (wait_subprocess_ms <= 0) {
        wait_subprocess_ms = DEFAULT_WAIT_SUBPROCESS;
    }

    restart_interval_ms = iniGetIntValue(NULL, "restart_interval_ms",
            iniContext, DEFAULT_RESTART_INTERVAL);
    if (restart_interval_ms < 0) {
        restart_interval_ms = DEFAULT_RESTART_INTERVAL;
    }

    check_alive_interval = iniGetIntValue(NULL, "check_alive_interval",
            iniContext, DEFAULT_CHECK_ALIVE_INTERVAL);
    if (check_alive_interval < 0) {
        check_alive_interval = DEFAULT_CHECK_ALIVE_INTERVAL;
    }

    enable_access_log = iniGetBoolValue(NULL, "enable_access_log", iniContext, false);

    logfiles_all = malloc(MAX_PATH_SIZE * iniContext->sections.item_count);
    acclogs_all = malloc(MAX_PATH_SIZE * iniContext->sections.item_count);
    if ((result=hash_walk(&iniContext->sections, ini_section_load, NULL)) != 0) {
        return result;
    }

    for (i = child_proc_array.count-1; i >= 0; i--) {
        if ((result=expand_child_cmd(child_proc_array.processes[i])) != 0) {
            return result;
        }
    }

    for (i = 0; i < child_proc_array.count; i++) {
        if ((result=process_set_command_params(child_proc_array.processes[i])) != 0) {
            return result;
        }
    }

    return parse_check_alive_commands();
}

static int setup_schedule_tasks()
{
#define SCHEDULE_ENTRIES_COUNT 2

    ScheduleEntry scheduleEntries[SCHEDULE_ENTRIES_COUNT];
    ScheduleArray scheduleArray;
    ScheduleEntry *pEntry;

    pEntry = scheduleEntries;
    memset(scheduleEntries, 0, sizeof(scheduleEntries));

    pEntry->id = sched_generate_next_id();
    pEntry->time_base.hour = 0;
    pEntry->time_base.minute = 0;
    pEntry->time_base.second = 0;
    pEntry->interval = 86400;
    pEntry->task_func = rotate_logs;
    pEntry->func_args = NULL;
    pEntry++;

    scheduleArray.entries = scheduleEntries;
    scheduleArray.count = pEntry - scheduleEntries;
    return sched_start(&scheduleArray, &schedule_tid,
        64 * 1024, (bool * volatile)&continue_flag);
}

static int update_process(int pid, const int status)
{
    ChildProcessInfo target;
    ChildProcessInfo *pTarget;
    ChildProcessInfo** found = NULL;

    target.pid = pid;
    pTarget = &target;
    found = (ChildProcessInfo **)bsearch(&pTarget, child_proc_array.processes,
        child_proc_array.count, sizeof(ChildProcessInfo *),
        process_info_cmp_pid);
    if (found == NULL) {
        if (cron_proc_array.count > 0) {
            found = (ChildProcessInfo **)bsearch(&pTarget,
                    cron_proc_array.processes, cron_proc_array.count,
                    sizeof(ChildProcessInfo *), process_info_cmp_pid);
        }
        if (found == NULL) {
            logError("file: "__FILE__", line: %d, pid: %d not found",
                    __LINE__, pid);
            return EINVAL;
        } else {
            logInfo("file: "__FILE__", line: %d, cron process exit "
                    "with status: %d. %s", __LINE__,
                    status, get_current_command(*found));
            return 0;
        }
    }

    if ((*found)->running) {
        (*found)->running = false;
        child_running--;
    }
    logInfo("file: "__FILE__", line: %d, process %d exit "
            "with status %d. running %d processes. %s",
            __LINE__, (*found)->pid, status, child_running,
            get_current_command(*found));
    return 0;
}

static int start_process(ChildProcessInfo *process)
{
    pid_t pid;
    CommandParams *command;

    command = get_next_command(process);
    pid = fork();
    if (pid == 0) { //child process
        const char *lfile;
        int fd;

        lfile = process->logfile;
        fd = open(lfile, O_APPEND | O_CREAT | O_WRONLY, 0644);
        umask(022);
        if (fd < 0) {
            logError("file: "__FILE__", line: %d, open file %s fail, "
                    "errno: %d, error info: %s", __LINE__, lfile,
                    errno, strerror(errno));
            _exit(1);
        }

        if (dup2(fd, 1) < 0 || dup2(fd, 2) < 0) {
            logError("file: "__FILE__", line: %d, dup2 fail, "
                    "errno: %d, error info: %s",
                    __LINE__, errno, strerror(errno));
            _exit(1);
        }
        if (execvp(command->argv[0], command->argv) < 0) {
            logError("file: "__FILE__", line: %d, execvp fail, "
                    "errno: %d, error info: %s",
                    __LINE__, errno, strerror(errno));
            _exit(1);
        }
    } else if (pid < 0) {
        logError("file: "__FILE__", line: %d, fork fail, "
                "errno: %d, error info: %s",
                __LINE__, errno, strerror(errno));
        return errno;
    } else {
        process->pid = pid;
    }

    return 0;
}

static int start_all_processes()
{
    int i;
    int result;
    int64_t now;

    now = get_current_time_ms();
    for (i = 0; i < child_proc_array.count; i++) {
        if (!child_proc_array.processes[i]->running
                && now - child_proc_array.processes[i]->last_start_time_ms >=
                child_proc_array.processes[i]->restart_interval_ms)
        {
            result = start_process(child_proc_array.processes[i]);
            if (result != 0) {
                return result;
            }

            child_proc_array.processes[i]->running = true;
            child_proc_array.processes[i]->last_start_time_ms = get_current_time_ms();
            child_running++;
            logInfo("file: "__FILE__", line: %d, process %d started%s."
                    " running %d processes. %s %s",
                    __LINE__, child_proc_array.processes[i]->pid,
                    get_current_command_entry(child_proc_array.processes[i])->
                    command.run_by_sh ? "(run by sh -c)" : "",
                    child_running, get_current_command(child_proc_array.processes[i]),
                    child_proc_array.processes[i]->enable_access_log ?
                    child_proc_array.processes[i]->acclog : "");
        }
    }

    if (child_proc_array.count > 1) {
        qsort(child_proc_array.processes, child_proc_array.count,
                sizeof(ChildProcessInfo *), process_info_cmp_pid);
    }
    return 0;
}

static int stop_all_processes()
{
    int i;
    long btime;
    btime = get_current_time_ms();
    for (i = 0; i < child_proc_array.count; i++) {
        ChildProcessInfo* pro = child_proc_array.processes[i];
        if (pro->pid > 0 && pro->running) {
            kill(pro->pid, SIGTERM);
        }
    }
    for (i = 0; i < wait_subprocess_ms/5 && child_running > 0; i++) {
        usleep(10*1000);
        check_all_processes();
    }

    if (child_running > 0) {
        for (i = 0; i < child_proc_array.count; i++) {
            ChildProcessInfo* pro = child_proc_array.processes[i];
            if (pro->pid > 0 && pro->running) {
                kill(pro->pid, SIGKILL);
            }
        }
    }
    for (i = 0; i < 10 && child_running > 0; i++) {
        usleep(10*1000);
        check_all_processes();
    }
    if (child_running > 0) {
        logWarning("file: "__FILE__", line: %d, after sigkill %d "
                "children still running. ignore",
                __LINE__, child_running);
        for (i = 0; i < child_proc_array.count; i++) {
            ChildProcessInfo* pro = child_proc_array.processes[i];
            if (pro->pid > 0 && pro->running) {
                child_proc_array.processes[i]->running = false;
                child_running--;
            }
        }
    }
    logInfo("file: "__FILE__", line: %d, all subprocesses stopped. "
            "used %ld ms, child_running %d", __LINE__,
            get_current_time_ms() - btime, child_running);
    return 0;
}

static void check_subproccess_alive()
{
    int i;
    CommandEntry *cmd_entry;

    if (child_running <= 0 || last_check_alive_time >= g_current_time) {
        return;
    }
    last_check_alive_time = g_current_time;

    for (i = 0; i < child_proc_array.count; i++) {
        ChildProcessInfo* child = child_proc_array.processes[i];
        if (!(child->pid > 0 && child->running && child->check_alive_interval > 0)) {
            continue;
        }

        if (child->last_check_alive_time + child->check_alive_interval > g_current_time) {
            continue;
        }

        child->last_check_alive_time = g_current_time;

        cmd_entry = get_current_command_entry(child);
        if (cmd_entry->health_check.type == hc_type_kill) {
            if (kill(child->pid, 0) != 0) {
                child->running = false;
                child_running--;
                logInfo("file: "__FILE__", line: %d, process %d "
                        "already exited. errno: %d, error info: %s, "
                        "running %d processes. %s", __LINE__,
                        child->pid, errno, strerror(errno),
                        child_running, get_current_command(child));
            }
        }
    }
}

static int check_all_processes()
{
    int pid;
    int status;

    while ((pid=waitpid(-1, &status, WNOHANG)) > 0) {
        if (!WIFSTOPPED(status)) {
            update_process(pid, status);
        } else {
            logDebug("pid: %d stopped", pid);
        }
    }

    return 0;
}

void rotate_file(const char* fname)
{
    time_t current_time;
    time_t deleted_time;
    struct tm tm;
    char old_filename[MAX_PATH_SIZE];
    int len;

    current_time  = get_current_time() - 60;
    deleted_time = current_time - log_file_keep_days * 86400;

    localtime_r(&current_time, &tm);
    memset(old_filename, 0, sizeof(old_filename));
    len = sprintf(old_filename, "%s.", fname);
    strftime(old_filename + len, sizeof(old_filename) - len,
        "%Y%m%d", &tm);
    if (access(old_filename, F_OK) == 0) {
        logError("file: "__FILE__", line: %d, "
                "file %s already exist, rotate failed. ignore",
                __LINE__, old_filename);
    } else if (rename(fname, old_filename) != 0) {
        logError("file: "__FILE__", line: %d, rename %s -> %s fail, "
                "errno: %d, error info: %s", __LINE__,
                fname, old_filename, errno, strerror(errno));
    }
    while(log_file_keep_days) {
        struct tm tm2;
        deleted_time -= 86400;
        localtime_r(&deleted_time, &tm2);
        memset(old_filename, 0, sizeof(old_filename));
        len = sprintf(old_filename, "%s.", fname);
        strftime(old_filename + len, sizeof(old_filename) - len,
            "%Y%m%d", &tm2);
        if (unlink(old_filename) != 0) {
            if (errno != ENOENT) {
                logError("file: "__FILE__", line: %d, "
                        "delete file %s fail, "
                        "errno: %d, error info: %s",
                        __LINE__, old_filename,
                        errno, strerror(errno));
            } else {
                break;
            }
        }
    }
}

static int rotate_logs(void* arg)
{
    int i;

    log_rotate(&g_log_context);
    if (log_file_keep_days) {
        log_delete_old_files(&g_log_context);
    }
    for (i = 0; i < logfiles_count; i ++) {
        rotate_file(logfiles_all[i]);
        if (enable_access_log && *acclogs_all[i] != '\0') {
            rotate_file(acclogs_all[i]);
        }
    }
    restart_subprocess = true;
    return 0;
}
