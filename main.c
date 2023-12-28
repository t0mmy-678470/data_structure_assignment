#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ev.h>
#include "nosql.h"

#define MAX_KEY_LEN 15
#define MAX_VALUE_LEN 30
#define LINE_LEN 150
#define TRUE 1
#define COMMAND_LEN 16

void run_redis_cb();
db * my_redis;
struct ev_loop *loop;
ev_io stdin_watcher;

int main(){
    my_redis = init(my_redis);
        
    loop = EV_DEFAULT;
    ev_io_init(&stdin_watcher, run_redis_cb, 0, EV_READ);
    ev_io_start(loop, &stdin_watcher);

    ev_run(loop, 0);
}

void run_redis_cb(){
    char cmd[COMMAND_LEN+1];
    char key[MAX_KEY_LEN+1];
    char hash_table_key[MAX_KEY_LEN+1];
    char value[MAX_VALUE_LEN+1];
    char line[LINE_LEN+2]; //多一個'\n'字元
    scanf("%16s", cmd);
        if(!strcmp(cmd, "SET") || !strcmp(cmd, "set")){
            scanf("%15s %30s", key, value);
            add_or_update_data(my_redis, key, value);
            // printf("OK\n");
        }
        else if(!strcmp(cmd, "GET") || !strcmp(cmd, "get")){
            scanf("%15s", key);
            char* result = get_data(my_redis, key);
            if(result){
                printf("\"%s\"\n", result);
            }
        }
        else if(!strcmp(cmd, "DEL") || !strcmp(cmd, "del")){
            scanf("%15s", key);
            if(delete_data(&my_redis, key)){
                printf("OK\n");
            }
            else{
                printf("key not found\n");
            }
        }
        else if(!strcmp(cmd, "LPUSH") || !strcmp(cmd, "lpush")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            int arr_len = 0;
            char** arr = (char**) malloc(sizeof(char*));
            char* data;
            data = strtok(line, " ");
            while(data!=NULL){
                arr_len++;
                arr = realloc(arr, arr_len*sizeof(char*));
                arr[arr_len-1] = (char*) malloc(sizeof(char)*strlen(data));
                strcpy(arr[arr_len-1], data);

                data = strtok(NULL, " ");
            }
            if(arr_len==0){
                printf("wrong number of argument\n");
                free(data);
                free(arr);
                return;
            }
            left_push(my_redis, key, arr, arr_len);
            for(int i=0;i<arr_len;i++){
                free(arr[i]);
            }
            free(arr);
            free(data);
        }
        else if(!strcmp(cmd, "RPUSH") || !strcmp(cmd, "rpush")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            int arr_len = 0;
            char** arr = (char**) malloc(sizeof(char*));
            char* data;
            data = strtok(line, " ");
            while(data!=NULL){
                arr_len++;
                arr = realloc(arr, arr_len*sizeof(char*));
                arr[arr_len-1] = (char*) malloc(sizeof(char)*strlen(data));
                strcpy(arr[arr_len-1], data);

                data = strtok(NULL, " ");
            }
            if(arr_len==0){
                printf("wrong number of argument\n");
                free(data);
                free(arr);
                return;
            }
            right_push(my_redis, key, arr, arr_len);
            for(int i=0;i<arr_len;i++){
                free(arr[i]);
            }
            free(arr);
            free(data);
        }
        else if(!strcmp(cmd, "LPOP") || !strcmp(cmd, "lpop")){
            scanf("%15s", key);
            char* result;
            result = left_pop(my_redis, key);
            if(result){
                printf("%s\n", result);
                free(result);
            }
        }
        else if(!strcmp(cmd, "RPOP") || !strcmp(cmd, "rpop")){
            scanf("%15s", key);
            char* result;
            result = right_pop(my_redis, key);
            if(result){
                printf("%s\n", result);
                free(result);
            }
        }
        else if(!strcmp(cmd, "LRANGE") || !strcmp(cmd, "lrange")){
            int start_idx, end_idx;
            scanf("%15s %d %d", key, &start_idx, &end_idx);
            list_range(my_redis, key, start_idx, end_idx);
        }
        else if(!strcmp(cmd, "LLEN") || !strcmp(cmd, "llen")){
            scanf("%15s", key);
            int len = list_len(my_redis, key);
            if(len == 0){
                printf("(empty list or set)\n");
            }
            else if(len == -1){
                printf("type error\n");
            }
            else if(len == -2){
                printf("key not found\n");
            }
            else{
                printf("%d\n", len);
            }
        }
        else if(!strcmp(cmd, "ZADD") || !strcmp(cmd, "zadd")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            int arr_len = 0;
            char** arr = (char**) malloc(sizeof(char*));
            int* arr_priority = (int*) malloc(sizeof(int));
            char* data;
            // key = strtok(line, " ");
            // int len_check = atoi(strtok(line, " "));
            data = strtok(line, " ");
            while(data!=NULL){
                arr_len++;
                arr_priority = realloc(arr_priority, (arr_len+1)/2*sizeof(int));
                arr_priority[(arr_len-1)/2] = atoi(data);
                data = strtok(NULL, " ");
                if(data == NULL){
                    // printf("wrong number of arguments\n");
                    break;
                }
                // 判斷data有沒有重複
                for(int i=0;i<(arr_len-1)/2;i++){
                    if (!strcmp(data, arr[i]))
                    {
                        arr_priority[i] = arr_priority[(arr_len-1)/2]; //儲存新的priority
                        arr_priority = realloc(arr_priority, (arr_len-1)/2*sizeof(int));
                        arr_len--;
                        // printf("arr_len=%d\n",arr_len);
                        goto endwhile;
                    }
                    
                }
                arr_len++;
                arr = realloc(arr, arr_len/2*sizeof(char*));
                arr[arr_len/2-1] = (char*) malloc(sizeof(char)*strlen(data));
                strcpy(arr[arr_len/2-1], data);
                // printf("hi\n");
                endwhile:
                {}
                data = strtok(NULL, " ");
            }
            if(arr_len&1==1 || arr_len==0){
                printf("arrlen = %d", arr_len);
                printf("wrong number of arguments\n");
                free(data);
                free(arr_priority);
                return;
            }
            set_add(my_redis, key, arr, arr_priority, arr_len/2);
            free(data);
            free(arr_priority);
        }
        else if(!strcmp(cmd, "ZCARD") || !strcmp(cmd, "zcard")){
            scanf("%15s", key);
            int result = set_card(my_redis, key);
            if(result == -2){
                printf("key not found\n");
            }
            else if(result == -1){
                printf("type error\n");
            }
            else{
                printf("%d\n", result);
            }
        }
        else if(!strcmp(cmd, "ZCOUNT") || !strcmp(cmd, "zcount")){
            scanf("%15s", key);
            int min, max;
            scanf("%d %d", &min, &max);
            int result = set_count(my_redis, key, min, max);
            if(result == -2){
                printf("type error\n");
            }
            else if(result == -1){
                printf("key not found\n");
            }
            else{
                printf("%d\n", result);
            }
        }
        else if(!strcmp(cmd, "ZRANGE") || !strcmp(cmd, "zrange")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            char* data;
            int start_idx, end_idx, withscores = 0;
            
            data = strtok(line, " ");
            if(data==NULL){
                printf("wrong number of arguments\n");
                return;
            }
            start_idx = atoi(data);

            data = strtok(NULL, " ");
            if(data==NULL){
                printf("wrong number of arguments\n");
                free(data);
                return;
            }
            end_idx = atoi(data);

            data = strtok(NULL, " ");
            if(data!=NULL){
                if(!strcmp(data, "WITHSCORES") || !strcmp(data, "withscores")){
                    withscores = 1;
                }
                else{
                    printf("syntax error\n");
                    // free(data);
                    return;
                }
            }
            // free(data);
            set_range(my_redis, key, start_idx, end_idx, withscores);
        }
        else if(!strcmp(cmd, "ZRANGEBYSCORE") || !strcmp(cmd, "zrangebyscore")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            char* data;
            int min, max, withscores = 0;
            
            data = strtok(line, " ");
            if(data==NULL){
                printf("wrong number of arguments\n");
                return;
            }
            min = atoi(data);

            data = strtok(NULL, " ");
            if(data==NULL){
                printf("wrong number of arguments\n");
                free(data);
                return;
            }
            max = atoi(data);

            data = strtok(NULL, " ");
            if(data!=NULL){
                if(!strcmp(data, "WITHSCORES") || !strcmp(data, "withscores")){
                    withscores = 1;
                }
                else{
                    printf("syntax error\n");
                    free(data);
                    return;
                }
            }
            free(data);
            set_range_by_score(my_redis, key, min, max, withscores);
        }
        else if(!strcmp(cmd, "ZRANK") || !strcmp(cmd, "zrank")){
            scanf("%15s %30s", key, value);
            int result = set_rank(my_redis, key, value);
            if(result == -3){
                printf("key not found\n");
            }
            else if(result == -2){
                printf("type error\n");
            }
            else if(result == -1){
                printf("member not found\n");
            }
            else{
                printf("%d\n", result);
            }
        }
        else if(!strcmp(cmd, "ZREM") || !strcmp(cmd, "zrem")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            int arr_len = 0;
            char** arr = (char**) malloc(sizeof(char*));
            char* data;
            data = strtok(line, " ");
            while(data!=NULL){
                arr_len++;
                arr = realloc(arr, arr_len*sizeof(char*));
                arr[arr_len-1] = (char*) malloc(sizeof(char)*strlen(data));
                strcpy(arr[arr_len-1], data);

                data = strtok(NULL, " ");
            }
            if(arr_len==0){
                printf("wrong number of argument\n");
                free(data);
                free(arr);
                return;
            }
            int result = set_remove(my_redis, key, arr, arr_len);
            if(result == -2){
                printf("wrong number of arguments\n");
            }
            else if(result == -1){
                printf("type error\n");
            }
            else{
                printf("%d\n", result);
            }

            free(data);
            free(arr);
        }
        else if(!strcmp(cmd, "ZREMRANGEBYSCORE") || !strcmp(cmd, "zremrangebyscore")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            char* data;
            int min, max, withscores = 0;
            
            data = strtok(line, " ");
            if(data==NULL){
                printf("wrong number of arguments\n");
                return;
            }
            min = atoi(data);

            data = strtok(NULL, " ");
            if(data==NULL){
                printf("wrong number of arguments\n");
                free(data);
                return;
            }
            max = atoi(data);

            // free(data);
            int result = set_remove_range_by_score(my_redis, key, min, max);
            if(result == -1){
                printf("type error\n");
            }
            else{
                printf("%d\n", result);
            }
        }
        else if(!strcmp(cmd, "ZINTERSTORE") || !strcmp(cmd, "zinterstore")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            int len, wrong_arg=0;
            char** arr = (char**) malloc(sizeof(char*));
            int* weights = (int*) malloc(sizeof(int));
            char* data;
            int no_weight = 0;
            data = strtok(line, " ");
            if(data == NULL){
                printf("wrong number of arguments1\n");
                free(arr);
                free(weights);
                return;
            }
            len = atoi(data);
            data = strtok(NULL, " ");
            for(int i=0;i<len;i++){
                if(data == NULL){
                    wrong_arg = 1;
                    break;
                }
                arr = realloc(arr, (i+1)*sizeof(char*));
                arr[i] = (char*) malloc(sizeof(char)*strlen(data));
                strcpy(arr[i], data);

                data = strtok(NULL, " ");
            }
            if(wrong_arg){
                printf("wrong number of arguments1\n");
                free(arr);
                free(weights);
                return;
            }

            // data = strtok(NULL, " ");
            if(!data){ //沒有weight
                weights = NULL;
                no_weight = 1;
                goto end_prepare_inter;
            }
            for(int i=0;i<len;i++){
                if(data == NULL){
                    wrong_arg = 1;
                    break;
                }
                weights = realloc(weights, (i+1)*sizeof(int));
                weights[i] = atoi(data);

                data = strtok(NULL, " ");
            }
            if(wrong_arg){
                printf("wrong number of arguments2\n");
                free(arr);
                free(weights);
                return;
            }

            end_prepare_inter:
            {}
            int result = set_interstore(my_redis, key, len, arr, weights);
            if(result == -1){
                printf("destination type error\n");
            }
            else{
                printf("%d\n", result);
            }
            free(arr);

            if(!no_weight){
                free(weights);
            }
            // set_add(my_redis, key, arr, arr_priority, arr_len);
        }
        else if(!strcmp(cmd, "ZUNIONSTORE") || !strcmp(cmd, "zunionstore")){
            scanf("%15s", key);
            fgets(line, LINE_LEN, stdin);
            line[strlen(line)-1] = '\0';
            int len, wrong_arg=0;
            char** arr = (char**) malloc(sizeof(char*));
            int* weights = (int*) malloc(sizeof(int));
            char* data;
            int no_weight = 0;
            data = strtok(line, " ");
            if(data == NULL){
                printf("wrong number of arguments\n");
                free(arr);
                free(weights);
                return;
            }
            len = atoi(data);
            data = strtok(NULL, " ");
            for(int i=0;i<len;i++){
                if(data == NULL){
                    wrong_arg = 1;
                    break;
                }
                arr = realloc(arr, (i+1)*sizeof(char*));
                arr[i] = (char*) malloc(sizeof(char)*strlen(data));
                strcpy(arr[i], data);

                data = strtok(NULL, " ");
            }
            if(wrong_arg){
                printf("wrong number of arguments\n");
                free(arr);
                free(weights);
                return;
            }

            // data = strtok(NULL, " ");
            if(!data){ //沒有weight
                weights = NULL;
                no_weight = 1;
                goto end_prepare_union;
            }
            for(int i=0;i<len;i++){
                if(data == NULL){
                    wrong_arg = 1;
                    break;
                }
                weights = realloc(weights, (i+1)*sizeof(int));
                weights[i] = atoi(data);

                data = strtok(NULL, " ");
            }
            if(wrong_arg){
                printf("wrong number of arguments\n");
                free(arr);
                free(weights);
                return;
            }
            
            end_prepare_union:
            {}
            int result = set_unionstore(my_redis, key, len, arr, weights);
            if(result == -1){
                printf("destination type error\n");
            }
            else{
                printf("%d\n", result);
            }
            free(arr);
            free(weights);
            // set_add(my_redis, key, arr, arr_priority, arr_len);
        }
        else if(!strcmp(cmd, "HSET") || !strcmp(cmd, "hset")){
            scanf("%15s %15s %30s", hash_table_key, key, value);
            int result = hash_set(my_redis, hash_table_key, key, value);
            if(result == -1){
                printf("type error\n");
            }
            else{
                printf("%d\n", result);
            }
        }
        else if(!strcmp(cmd, "HGET") || !strcmp(cmd, "hget")){
            scanf("%15s %15s", hash_table_key, key);
            char* result = hash_get(my_redis, hash_table_key, key);
            if(result){
                printf("%s\n", result);
            }
        }
        else if(!strcmp(cmd, "HDEL") || !strcmp(cmd, "hdel")){
            scanf("%15s %15s", hash_table_key, key);
            int result = hash_del(my_redis, hash_table_key, key);
            if(result==-1){
                printf("type error\n");
            }
            else{
                printf("%d\n", result);
            }
        }
        else if(!strcmp(cmd, "EXIT") || !strcmp(cmd, "exit")){
            // break;
            ev_break(EV_A_ EVBREAK_ALL);
        }
        else{
            printf("command not found\n");
        }
}