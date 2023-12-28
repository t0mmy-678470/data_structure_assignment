#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "nosql.h"
#define MAX_KEY_LEN 15
#define MAX_VALUE_LEN 30
#define TRUE 1
#define COMMAND_LEN 1
#define TYPE_STRING 1
#define TYPE_LIST 2
#define TYPE_SET 3
#define TYPE_HASH_TABLE 4
#define TYPE_NONE 5
#define LOAD_FACTOR 0.7
// #define INIT_HASH_TABLE_SIZE 10

int hash(char* key, int hash_table_size){
    int c, sum=0;
    while(c=*key++){
        sum+=c;
    }
    return sum % hash_table_size;     
}

void free_hashed_table(HASH_NODE ** table){
    HASH_NODE* hash_node;
    while(hash_node = *table++){
        HASH_NODE* pre_node;
        while(hash_node){
            pre_node = hash_node;
            hash_node = hash_node->next;
            free(pre_node->field);
            free(pre_node->value);
            free(pre_node);
        }
    }
    printf("table NULL? %d\n", table==NULL);
    // free(table);
}

void init_hash_node(HASH_NODE* hash_node, char* field, char* value){
    // hash_node = (HASH_NODE*) malloc(sizeof(HASH_NODE));
    hash_node->field = (char*) malloc(sizeof(char)*(strlen(field)+1));
    hash_node->value = (char*) malloc(sizeof(char)*(strlen(value)+1));
    strcpy(hash_node->field, field);
    strcpy(hash_node->value, value);
    hash_node->next = NULL;
}

void resize_hash_table(HASH_TABLE* hash_table){
    int new_size = hash_table->table_size * 2;
    HASH_NODE** new_table = (HASH_NODE**) calloc(new_size, sizeof(HASH_NODE*));
    for(int i=0;i<hash_table->table_size;i++){ //把舊table的資料搬運到新table
        HASH_NODE* tmp_hash_node = hash_table->table[i];
        while(tmp_hash_node){
            HASH_NODE* appended_hash_node;
            while(appended_hash_node){
                appended_hash_node = appended_hash_node->next;
            }
            appended_hash_node = (HASH_NODE*) malloc(sizeof(HASH_NODE));
            init_hash_node(appended_hash_node, tmp_hash_node->field, tmp_hash_node->value);
            new_table[hash(tmp_hash_node->field,new_size)] = appended_hash_node;
            tmp_hash_node = tmp_hash_node->next;
        }
    }
    free_hashed_table(hash_table->table);
    free(hash_table->table);
    hash_table->table = new_table;
    hash_table->table_size = new_size;
}

int hash_del(db* nosqldb, char* hash_table_name, char* field){
    db *tmp_node = nosqldb;
    HASH_TABLE* tmp_hash_table;
    int found_table = 0;
    while(tmp_node->next != NULL) {        
        //判斷hash_table有沒有存在
        if(!strcmp(tmp_node->key, hash_table_name)){
            if(tmp_node->value_type != TYPE_HASH_TABLE){
                return -1; //type error
            }
            found_table = 1;
            tmp_hash_table = tmp_node->value.hash_table;
            break;
        }
        tmp_node = tmp_node->next;
    }
    if(!found_table){
        return 0;
    }
    HASH_NODE* hash_node = tmp_hash_table->table[hash(field, tmp_hash_table->table_size)];
    HASH_NODE* pre_hash_node = NULL;
    while(hash_node){
        if(!strcmp(hash_node->field, field)){
            if(pre_hash_node){
                pre_hash_node->next = hash_node->next;
            }
            else{
                tmp_hash_table->table[hash(field, tmp_hash_table->table_size)] = hash_node->next;
            }
            free(hash_node->field);
            free(hash_node->value);
            free(hash_node);
            return 1;
        }
        pre_hash_node = hash_node;
        hash_node = hash_node->next;
    }
    return 0;
}

char* hash_get(db* nosqldb, char* hash_table_name /*node的key*/, char* field){
    db *tmp_node = nosqldb;
    HASH_TABLE* tmp_hash_table;
    int found_table = 0;
    while(tmp_node->next != NULL) {        
        //判斷hash_table有沒有存在
        if(!strcmp(tmp_node->key, hash_table_name)){
            if(tmp_node->value_type != TYPE_HASH_TABLE){
                printf("type error\n");
                return NULL; //type error
            }
            found_table = 1;
            tmp_hash_table = tmp_node->value.hash_table;
            break;
        }
        tmp_node = tmp_node->next;
    }
    if(!found_table){
        printf("NULL\n");
        return NULL;
    }
    HASH_NODE* hash_node = tmp_hash_table->table[hash(field, tmp_hash_table->table_size)];
    while(hash_node){
        if(!strcmp(hash_node->field, field)){
            return hash_node->value;
        }
        hash_node = hash_node->next;
    }
    printf("NULL\n");
    return NULL;
}

int hash_set(db* nosqldb, char* hash_table_name /*node的key*/, char* field, char* value){
    db *tmp_node = nosqldb;
    HASH_TABLE* tmp_hash_table;
    int found_table = 0;
    while(tmp_node->next != NULL) {        
        //判斷hash_table有沒有存在
        if(!strcmp(tmp_node->key, hash_table_name)){
            if(tmp_node->value_type != TYPE_HASH_TABLE){
                return -1; //type error
            }
            found_table = 1;
            tmp_hash_table = tmp_node->value.hash_table;
            break;
        }
        tmp_node = tmp_node->next;
    }
    if(found_table){ //找到table，要找有沒有該feild
        int key_idx = hash(field, tmp_hash_table->table_size);
        HASH_NODE* tmp_hash_node = tmp_hash_table->table[key_idx];
        HASH_NODE* pre_hash_node = NULL;
        while(tmp_hash_node){
            if(!strcmp(tmp_hash_node->field, field)){
                tmp_hash_node->value = (char*) realloc(tmp_hash_node->value, (strlen(value)+1)*sizeof(char));
                strcpy(tmp_hash_node->value, value);
                return 0; //update field
            }
            pre_hash_node = tmp_hash_node;
            tmp_hash_node->next;
        }
        if(!pre_hash_node){ //hash table該index為空
            tmp_hash_table->table[key_idx] = (HASH_NODE*) malloc(sizeof(HASH_NODE));
            init_hash_node(tmp_hash_table->table[key_idx], field, value);
            tmp_hash_table->num_node++;
            // 判斷load factor
            if((float)tmp_hash_table->num_node/tmp_hash_table->table_size > LOAD_FACTOR){
                printf("%f\n", (float)tmp_hash_table->num_node/tmp_hash_table->table_size);
                resize_hash_table(tmp_hash_table);
            }
            return 1; //創新field
        }
        else{
            pre_hash_node->next = (HASH_NODE*) malloc(sizeof(HASH_NODE));
            init_hash_node(pre_hash_node->next, field, value);
            return 1; //創新field
        }
    }
    // 沒找到hash table
    tmp_node->key = (char*) malloc(sizeof(char)*(strlen(hash_table_name)+1));
    strcpy(tmp_node->key, hash_table_name);
    tmp_node->value_type = TYPE_HASH_TABLE;
    tmp_node->value.hash_table = (HASH_TABLE*) malloc(sizeof(HASH_TABLE));
    tmp_node->value.hash_table->table_size = INIT_HASH_TABLE_SIZE;
    tmp_node->value.hash_table->num_node = 1;
    tmp_node->value.hash_table->table = (HASH_NODE**) calloc(INIT_HASH_TABLE_SIZE, sizeof(HASH_NODE*));
    // memset(tmp_node->value.hash_table->table, NULL, INIT_HASH_TABLE_SIZE*sizeof(HASH_NODE*)); //把hash table的值預設為NULL
    HASH_NODE* hash_node = (HASH_NODE*) malloc(sizeof(HASH_NODE));
    init_hash_node(hash_node, field, value);
    tmp_node->value.hash_table->table[hash(field, INIT_HASH_TABLE_SIZE)] = hash_node;

    tmp_node->next = init(tmp_node->next);

    return 1;
}

SET* init_set(SET* node, char* new_data, int prioroty){
    node = (SET*) malloc(sizeof(SET));
    node->data = (char*) malloc(sizeof(char) * strlen(new_data));
    strcpy(node->data, new_data);
    node->priority = prioroty;
    node->pre = node;
    node->next = node;
    return node;
}

SET* insert_set_value_before(SET* node, char* new_data, int priority){
    SET* new_list_node = (SET*) malloc(sizeof(SET));
    new_list_node->data = (char*) malloc(sizeof(char) * strlen(new_data));
    strcpy(new_list_node->data, new_data);
    new_list_node->priority = priority;
    new_list_node->pre = node->pre;
    node->pre->next = new_list_node;
    new_list_node->next = node;
    node->pre = new_list_node;
    node = new_list_node;
    return node;
}

void set_add(db* nosqldb, char* key, char** arr, int* arr_priority, int len){
    db *tmp_node = nosqldb;
    // printf("len = %d\n", len);
    if(len == 0){
        printf("wrong number of arguments\n");
        return;
    }
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node !!!!!len不一樣比較
            tmp_node = tmp_node->next;
        }
        else{ // update
            if(tmp_node->value_type != TYPE_SET){
                printf("type error\n");
            }
            else { //udpate set
                for(int i=0;i<len;i++){ //遍歷要新增的資料
                    SET* tmp_set = tmp_node->value.set;
                    int del_first = 1;
                    int set_len = set_card(nosqldb, key);
                    if(set_len == 0){
                        break;
                    }
                    do{ //遍歷set刪除一樣data的資料
                        if(!strcmp(tmp_set->data, arr[i])){ //一樣的資料
                            if(set_len == 1){
                                free(tmp_set);
                                // break;
                            }
                            else if(del_first){
                                tmp_node->value.set = tmp_node->value.set->next;
                                tmp_set->pre->next = tmp_set->next;
                                tmp_set->next->pre = tmp_set->pre;
                                free(tmp_set);
                            }
                            else{
                                tmp_set->pre->next = tmp_set->next;
                                tmp_set->next->pre = tmp_set->pre;
                                free(tmp_set);
                                // break;
                            }
                            break;
                        }
                        else{
                            tmp_set = tmp_set->next;
                            del_first = 0;
                        }
                    } while (tmp_set != tmp_node->value.set);
                    if(!tmp_node->value.set){
                        break;
                    }
                }

                int begin = 0;
                if(tmp_node->value.set == NULL){
                    tmp_node->value.set = init_set(tmp_node->value.set, arr[0], arr_priority[0]);
                    begin++;
                }
                for(int i=begin;i<len;i++){ //逐一新增data到set
                    SET* tmp_set = tmp_node->value.set;
                    int change_first = 1; //set 的第一個node要指向新data
                    tmp_set = tmp_node->value.set;
                    do{ //遍歷set找到適合的位置加入
                        if(tmp_set->priority<arr_priority[i]){
                            change_first = 0;
                            tmp_set = tmp_set->next;
                        }
                        else if(tmp_set->priority == arr_priority[i]){
                            if(strcmp(tmp_set->data, arr[i])>0){
                                break;
                            }
                            else{
                                change_first = 0;
                                tmp_set = tmp_set->next;
                            }
                        }
                        else{
                            // if(tmp_set == tmp_node->value.set){
                            //     change_first = 1;
                            // }
                            break;
                        }
                    }while(tmp_set != tmp_node->value.set);
                    if(change_first){
                        tmp_node->value.set = insert_set_value_before(tmp_set, arr[i], arr_priority[i]);
                    }
                    else{
                        insert_set_value_before(tmp_set, arr[i], arr_priority[i]);
                    }
                }
                printf("OK\n");
            }
            return;
        }
    }
    
    // add new node
    tmp_node->key = (char*) malloc( strlen(key) );
    strcpy(tmp_node->key, key);

    tmp_node->value_type = TYPE_SET;
    
    tmp_node->value.set  = init_set(tmp_node->value.set, arr[0], arr_priority[0]);
    for(int i=1;i<len;i++){ //逐一新增data
        SET* tmp_set = tmp_node->value.set;
        int change_first = 0; //set 的第一個node要指向新data
        // printf("in front of while loop\n");
        do{
            if(tmp_set->priority<arr_priority[i]){
                // printf("here1\n");
                tmp_set = tmp_set->next;
                // printf("in if2\n");
            }
            else if(tmp_set->priority == arr_priority[i]){
                // printf("here2\n");
                if(strcmp(tmp_set->data, arr[i])>0){
                    break;
                }
                else{
                    tmp_set = tmp_set->next;
                }
            }
            else{
                // printf("here3\n");
                if(tmp_set == tmp_node->value.set){
                    change_first = 1;
                }
                break;
            }
            // printf("in while loop\n");
        }while(tmp_set != tmp_node->value.set);
        // printf("at the end of while loop\n");
        if(change_first){
            tmp_node->value.set = insert_set_value_before(tmp_set, arr[i], arr_priority[i]);
        }
        else{
            insert_set_value_before(tmp_set, arr[i], arr_priority[i]);
        }
    }
    tmp_node->next = init(tmp_node->next);
    printf("OK\n");
}

int set_card(db* nosqldb, char* key){ //給出set有多少node
    db *tmp_node = nosqldb;
    
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{
            if(tmp_node->value.set!=NULL){
                SET* tmp_set_node = tmp_node->value.set; 
                int len = 1;
                
                while(tmp_set_node->next!=tmp_node->value.set){
                    len++;
                    tmp_set_node = tmp_set_node->next;
                }
                return len;
            }
            else if(tmp_node->value_type != TYPE_SET){
                return -1; //type error
            }
            else{
                return 0; //empty
            }
        }
    }
    return -2; //key not found
}

int set_count(db* nosqldb, char* key, int min, int max){  //找出該set在給定priority區間有多少node
    db *tmp_node = nosqldb;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{
            if(tmp_node->value_type != TYPE_SET){
                // printf("type error\n");
                return -2; //type error
            }
            else if(tmp_node->value.set){
                SET* tmp_set_node = tmp_node->value.set;
                int count = 0;
                do{
                    if(min<=tmp_set_node->priority && tmp_set_node->priority<=max){
                        count++;
                    }
                    else if(tmp_set_node->priority>max){
                        return count;
                    }
                    tmp_set_node = tmp_set_node->next;
                }while(tmp_set_node != tmp_node->value.set);
                return count;
            }
            else{
                return 0;
            }
        }
        // printf("are you still here?\n");
    }
    return -1; //key not found
}

void set_range(db* nosqldb, char* key, int start_idx, int end_idx, int withscore){
    db *tmp_node = nosqldb;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{
            if(tmp_node->value_type != TYPE_SET){
                printf("type error\n");
            }
            else if(tmp_node->value.set){
                SET* tmp_set_node = tmp_node->value.set;
                int len = set_card(nosqldb, key);
                if(end_idx == -1){ //看到最後
                    end_idx = len-1;
                }
                for(int i=0;i<=end_idx; i++){
                    if(len-1 < i || (tmp_set_node==tmp_node->value.set && i>0)){
                        // printf("break?\n");
                        break;
                    }
                    if(i<start_idx){
                        tmp_set_node = tmp_set_node->next;
                        continue;
                    }
                    if(withscore){
                        printf("%d) \"%s\"\n",(i-start_idx+1)*2-1, tmp_set_node->data);
                        printf("%d) \"%d\"\n",(i-start_idx+1)*2, tmp_set_node->priority);
                    }
                    else{
                        printf("%d) \"%s\"\n",i-start_idx+1, tmp_set_node->data);
                    }
                    tmp_set_node = tmp_set_node->next;
                }
                printf("\n");  
            }
            else{
                printf("set is empty\n");
            }
            return;
        }
        // printf("are you still here?\n");
    }
    printf("key not found\n");
}

void set_range_by_score(db* nosqldb, char* key, int min, int max, int withscore){
    db *tmp_node = nosqldb;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{
            if(tmp_node->value_type != TYPE_SET){
                printf("type error\n");
            }
            else if(tmp_node->value.set){
                SET* tmp_set_node = tmp_node->value.set;
                int num=0;
                do{
                    if(min<=tmp_set_node->priority && tmp_set_node->priority<=max){
                        if(withscore){
                            printf("%d) \"%s\"\n",++num, tmp_set_node->data);
                            printf("%d) \"%d\"\n",++num, tmp_set_node->priority);
                        }
                        else{
                            printf("%d) \"%s\"\n",++num, tmp_set_node->data);
                        }
                    }
                    else if(tmp_set_node->priority>max){
                        return;
                    }
                    tmp_set_node = tmp_set_node->next;
                }while(tmp_set_node != tmp_node->value.set);
            }
            return;
        }
    }
    printf("key not found");
}

int set_rank(db* nosqldb, char* key, char* data){ //給出set該key的index
    db *tmp_node = nosqldb;
    
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{
            if(tmp_node->value.set!=NULL){
                SET* tmp_set_node = tmp_node->value.set; 
                int rank = 0;
                do{
                    if(!strcmp(tmp_set_node->data,data)){
                        return rank;
                    }
                    else{
                        rank++;
                        tmp_set_node = tmp_set_node->next;
                    }
                }while(tmp_set_node != tmp_node->value.set);
                return -1; //member not found
            }
            else if(tmp_node->value_type != TYPE_SET){
                return -2; //type error
            }
        }
    }
    return -3; //key not found
}

int set_remove(db* nosqldb, char* key, char** arr, int len){
    db *tmp_node = nosqldb;
    
    if(len == 0){
        return -2; //wrong number of arguments
    }
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node !!!!!len不一樣比較
            tmp_node = tmp_node->next;
        }
        else{ 
            if(tmp_node->value_type != TYPE_SET){
                return -1; //type error
            }
            else if(tmp_node->value.set){ //set != NULL
                SET* tmp_set_node = tmp_node->value.set;
                int count = 0;
                begin:  //用continue有bug
                do{ //traversal set 的所有node
                    // if()
                    int conti = 0;
                    SET* del = NULL; //要被刪的node
                    for(int i=0;i<len;i++){
                        if(!strcmp(tmp_set_node->data, arr[i])){
                            tmp_set_node->pre->next = tmp_set_node->next;
                            tmp_set_node->next->pre = tmp_set_node->pre;
                            del = tmp_set_node;
                            count++;
                            break;
                        }
                    }
                    tmp_set_node = tmp_set_node->next;
                    
                    // printf("del==NULL: %d\n",del==NULL);
                    if(del){ //zcard==1
                        if(tmp_node->value.set->next == tmp_node->value.set){ //如果最後一個元素被刪掉
                            tmp_node->value.set = NULL;
                            break;
                        }
                        if(del == tmp_node->value.set){ //刪到第一個元素
                            // printf("deleted data: %s",del->data);
                            conti = 1;
                            tmp_node->value.set = tmp_node->value.set->next;
                        }
                        free(del->data);
                        free(del);

                        if(conti){
                            // continue;
                            goto begin;
                        }
                    }
                    if(count>=len){
                        return count;
                    }
                } while(tmp_set_node!=tmp_node->value.set);
                return count;
            }
            else{
                return 0;
            }
        }
    }
}

int set_remove_range_by_score(db* nosqldb, char* key, int min, int max){
    db *tmp_node = nosqldb;
    
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node !!!!!len不一樣比較
            tmp_node = tmp_node->next;
        }
        else{ 
            if(tmp_node->value_type != TYPE_SET){
                return -1; //type error
            }
            else if(tmp_node->value.set){ //set != NULL
                SET* tmp_set_node = tmp_node->value.set;
                int count = 0;
                begin:  //用continue有bug
                do{ //traversal set 的所有node
                    int conti = 0;
                    SET* del = NULL; //要被刪的node
                    
                    if(tmp_set_node->priority < min){
                        tmp_set_node = tmp_set_node->next;
                        continue; //跳到下面的while判斷
                    }
                    else if(min <= tmp_set_node->priority && tmp_set_node->priority <= max){
                        tmp_set_node->pre->next = tmp_set_node->next;
                        tmp_set_node->next->pre = tmp_set_node->pre;
                        del = tmp_set_node;
                        count++;
                    }
                    else{
                        break;
                    }
                    
                    tmp_set_node = tmp_set_node->next;
                    
                    // printf("del==NULL: %d\n",del==NULL);
                    if(del){ //zcard==1
                        if(tmp_node->value.set->next == tmp_node->value.set){ //如果最後一個元素被刪掉
                            tmp_node->value.set = NULL;
                            break;
                        }
                        if(del == tmp_node->value.set){ //刪到第一個元素
                            // printf("deleted data: %s",del->data);
                            conti = 1;
                            tmp_node->value.set = tmp_node->value.set->next;
                        }
                        free(del->data);
                        free(del);

                        if(conti){
                            // continue;
                            goto begin;
                        }
                    }

                } while(tmp_set_node!=tmp_node->value.set);
                return count;
            }
            else{
                return 0;
            }
        }
    }
}

int set_interstore(db* nosqldb, char* dst_key, int numkeys, char** keys, int* weights){
    db* trail_node = nosqldb;
    SET** sets = (SET**) malloc(sizeof(SET*)*numkeys);
    int key_found = 0;
    int found_dst_key = 0;
    int com_member = 0;
    db* dst_node = NULL;
    SET* dst_set = NULL;
    SET* last_dst_set_member = NULL;
    while(trail_node->next != NULL) { //search every node to find whether have the same key existed
        for(int i=0;i<numkeys;i++){ //找要做交集的node
            if(!strcmp(trail_node->key, keys[i])){
                if(trail_node->value.set){
                    sets[key_found] = trail_node->value.set;
                }else{
                    free(sets);
                    return 0;
                }
                key_found++;
                break;
            }
        }
        if(!strcmp(trail_node->key, dst_key)){ //找dst node
            found_dst_key = 1;
            if(trail_node->value_type == TYPE_SET){
                dst_node = trail_node;
            }
            else{
                return -1; //dst type error
            }
        }
        if(key_found == numkeys && found_dst_key){
            break;
        }

        trail_node = trail_node->next;
    }
    
    if(key_found<numkeys){
        free(sets);
        // printf("here?");
        return 0; //有key沒被找到
    }
    SET* set0 = sets[0]; //交集的第一個set


    if(!weights){ //如果沒有給權重
        // printf("hereeererrererererer\n");
        weights = (int*) malloc(sizeof(int)*numkeys);
        for(int i=0;i<numkeys;i++){
            weights[i] = 1;
        }
    }
    // printf("weights[0]=%d\n", weights[0]);

    do{ //儲存所有交集在dst_set
        char* data = (char*)malloc(sizeof(char)*strlen(set0->data));
        strcpy(data, set0->data);
        int priority = set0->priority * weights[0];
        int inter = 1;
        for(int i=1;i<numkeys;i++){ //一個一個set看
            SET* tmp_set_i = sets[i];
            int found_com_on_set_i = 0;
            do{
                if(!strcmp(set0->data, tmp_set_i->data)){  //儲存的資料一樣
                    // printf("before priority=%d\n", priority);
                    priority+=tmp_set_i->priority * weights[i];
                    // printf("after priority=%d\n", priority);
                    // printf("weights[0]=%d\n", weights[0]);
                    // printf("sets[i]->data=%s\nsets[i]->priority=%d\n", sets[i]->data, sets[i]->priority);
                    found_com_on_set_i = 1;
                    break;
                }
                else{
                    tmp_set_i = tmp_set_i->next;
                }
            }while(tmp_set_i != sets[i]);

            if(!found_com_on_set_i){
                inter = 0;
                break;
            }
            
        }
        if(inter){ //是交集，將資料存起來
            // printf("2 priority=%d\n", priority);
            com_member++;
            if(dst_set == NULL){
                dst_set = init_set(dst_set, data, priority);
                // last_dst_set_member = dst_set;
            }
            else{ //dst_set加一個node
                // -------------------------------
                SET* tmp_set = dst_set;
                int change_first = 0; //set 的第一個node要指向新data
                do{ //遍歷dst_set
                    if(tmp_set->priority<priority){
                        tmp_set = tmp_set->next;
                    }
                    else if(tmp_set->priority == priority){
                        if(strcmp(tmp_set->data, data)>0){
                            break; //找到加的位置了
                        }
                        else{
                            tmp_set = tmp_set->next;
                        }
                    }
                    else{
                        if(tmp_set == dst_set){
                            change_first = 1;
                        }
                        break;
                    }
                }while(tmp_set != dst_set);
                if(change_first){
                    dst_set = insert_set_value_before(tmp_set, data, priority);
                }
                else{
                    insert_set_value_before(tmp_set, data, priority);
                }
                // -------------------------------
            }
        }

        set0 = set0->next;
    }while(set0 != sets[0]);

    free(sets);
    // printf("common member=%d\n", com_member);
    if(com_member == 0){
        return 0;
    }
    // dst_set做好了，要把它加到資料庫中
    if(dst_node){ //先清空dst_node
        SET* old_dst_set = dst_node->value.set;
        SET* pre_set = NULL;
        SET* tmp_dst_set = old_dst_set;
        do{
            pre_set = tmp_dst_set;
            tmp_dst_set = tmp_dst_set->next;
            free(pre_set);
        }while(tmp_dst_set != old_dst_set);

        dst_node->value.set = dst_set;
    }
    else{
        dst_node = (db*) malloc(sizeof(db));
        dst_node->key = (char*) malloc( strlen(dst_key) );
        strcpy(dst_node->key, dst_key);
        dst_node->value.set = dst_set;
        dst_node->value_type = TYPE_SET;

        dst_node->next = nosqldb->next;
        nosqldb->next = dst_node;
    }

    return com_member;
}

int set_unionstore(db* nosqldb, char* dst_key, int numkeys, char** keys, int* weights){
    db* trail_node = nosqldb;
    SET** sets = (SET**) malloc(sizeof(SET*)*numkeys);
    int key_found = 0;
    int found_dst_key = 0;
    int com_member = 0;
    db* dst_node = NULL;
    SET* dst_set = NULL;
    SET* last_dst_set_member = NULL;
    while(trail_node->next != NULL) { //search every node to find whether have the same key existed
        for(int i=0;i<numkeys;i++){ //找要做交集的node
            if(!strcmp(trail_node->key, keys[i])){
                if(trail_node->value.set){
                    sets[key_found] = trail_node->value.set;
                }else{
                    free(sets);
                    return 0;
                }
                key_found++;
                break;
            }
        }
        if(!strcmp(trail_node->key, dst_key)){ //找dst node
            found_dst_key = 1;
            if(trail_node->value_type == TYPE_SET){
                dst_node = trail_node;
            }
            else{
                return -1; //destination type error
            }
        }
        if(key_found == numkeys && found_dst_key){
            break;
        }
        trail_node = trail_node->next;
        
    }

    if(!weights){ //如果沒有給權重
        weights = (int*) malloc(sizeof(int)*numkeys);
        for(int i=0;i<numkeys;i++){
            weights[i] = 1;
        }
    }

    //儲存所有聯集在dst_set
    for(int i=0;i<numkeys;i++){
        SET* set_i = sets[i];
        do{
            char* data = (char*) malloc(sizeof(char) * strlen(set_i->data));
            strcpy(data, set_i->data);
            int priority = set_i->priority;
            com_member++; // ! ! !
            if(dst_set == NULL){ //交集的set是空的
                dst_set = init_set(dst_set, data, priority);
                // last_dst_set_member = dst_set;
            }
            else{ //dst_set加一個node
                SET* tmp_set = dst_set;
                int change_first = 0; //set 的第一個node要指向新data
                int no_insert = 0;
                do{ // traversal交集的set，"判斷"是否要插入新set node
                    if(!strcmp(tmp_set->data, data)){ //有一樣的資料
                        free(data);
                        tmp_set->priority += priority * weights[i];
                        no_insert = 1;
                        com_member--;
                        break;
                    }
                    else{
                        tmp_set = tmp_set->next;
                    }
                }while(tmp_set != dst_set);

                if(no_insert){
                    set_i = set_i->next;
                    continue;
                }

                // tmp_set == dst_set;  // ? ? ? ?
                do{ //遍歷dst_set找到適當的位置插入新資料
                    if(tmp_set->priority<priority){
                        tmp_set = tmp_set->next;
                    }
                    else if(tmp_set->priority == priority){
                        if(strcmp(tmp_set->data, data)>0){
                            break; //找到加的位置了
                        }
                        else{
                            tmp_set = tmp_set->next;
                        }
                    }
                    else{
                        if(tmp_set == dst_set){
                            change_first = 1;
                        }
                        break;
                    }
                }while(tmp_set != dst_set);
                if(change_first){
                    dst_set = insert_set_value_before(tmp_set, data, priority);
                }
                else{
                    insert_set_value_before(tmp_set, data, priority);
                }
            }

            set_i = set_i->next;
        }while(sets[i] != set_i);
        char* data;
        int priority;
    }
// ------------------------------------------------------------------

    free(sets);
    if(com_member == 0){
        return 0;
    }
    // dst_set做好了，要把它加到資料庫中
    if(dst_node){ //先清空dst_node
        SET* old_dst_set = dst_node->value.set;
        SET* pre_set = NULL;
        SET* tmp_dst_set = old_dst_set;
        do{
            pre_set = tmp_dst_set;
            tmp_dst_set = tmp_dst_set->next;
            free(pre_set);
        }while(tmp_dst_set != old_dst_set);

        dst_node->value.set = dst_set;
    }
    else{
        dst_node = (db*) malloc(sizeof(db));
        dst_node->key = (char*) malloc( strlen(dst_key) );
        strcpy(dst_node->key, dst_key);
        dst_node->value.set = dst_set;
        dst_node->value_type = TYPE_SET;

        dst_node->next = nosqldb->next;
        nosqldb->next = dst_node;
    }

    return com_member;
}

int list_len(db* nosqldb, char* key){
    db *tmp_node = nosqldb;
    
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{
            if(tmp_node->value.list!=NULL){
                LIST* tmp_list_node = tmp_node->value.list; 
                int len = 1;
                
                while(tmp_list_node->next!=tmp_node->value.list){
                    len++;
                    tmp_list_node = tmp_list_node->next;
                }
                return len;
            }
            else if(tmp_node->value_type != TYPE_LIST){
                return -1; //type error
            }
            else{
                return 0; //empty
            }
        }
    }
    return -2; //key not found
}

LIST* insert_list_value_before(LIST* node, char* new_data){
    LIST* new_list_node = (LIST*) malloc(sizeof(LIST));
    new_list_node->data = (char*) malloc(sizeof(char) * strlen(new_data));
    strcpy(new_list_node->data, new_data);
    new_list_node->pre = node->pre;
    node->pre->next = new_list_node;
    new_list_node->next = node;
    node->pre = new_list_node;
    node = new_list_node;
    return node;
}

LIST* insert_list_value_after(LIST* node, char* new_data){
    LIST* new_list_node = (LIST*) malloc(sizeof(LIST));
    new_list_node->data = (char*) malloc(sizeof(char) * strlen(new_data));
    strcpy(new_list_node->data, new_data);
    node->next->pre = new_list_node;
    new_list_node->pre = node;
    new_list_node->next = node->next;
    node->next = new_list_node;
    node = node->next;
    return node;
}

LIST* init_list(LIST* node, char* new_data){
    node = (LIST*) malloc(sizeof(LIST));
    node->data = (char*) malloc(sizeof(char) * strlen(new_data));
    strcpy(node->data, new_data);
    node->pre = node;
    node->next = node;
    return node;
}

void left_push(db* nosqldb, char* key, char** arr, int len){
    db *tmp_node = nosqldb;
    
    if(len == 0){
        printf("wrong number of arguments\n");
        return;
    }
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node !!!!!len不一樣比較
            tmp_node = tmp_node->next;
        }
        else{ // update
            if(tmp_node->value_type != TYPE_LIST){
                printf("type error\n");
            }
            else if(tmp_node->value.list){ //string == NULL
                for(int i=0;i<len;i++){
                    tmp_node->value.list = insert_list_value_before(tmp_node->value.list, arr[i]);
                }
                printf("OK\n");
            }
            else{
                for(int i=0;i<len;i++){
                    if(tmp_node->value.list == NULL){
                        tmp_node->value.list  = init_list(tmp_node->value.list, arr[i]);
                    }
                    else{
                        tmp_node->value.list = insert_list_value_before(tmp_node->value.list, arr[i]);
                    }
                }
                printf("OK\n");
            }
            return;
        }
    }
    // add new node
    tmp_node->key = (char*) malloc( strlen(key) );
    strcpy(tmp_node->key, key);

    for(int i=0;i<len;i++){
        if(tmp_node->value.list == NULL){
            tmp_node->value_type = TYPE_LIST;
            tmp_node->value.list  = init_list(tmp_node->value.list, arr[i]);
        }
        else{
            tmp_node->value.list = insert_list_value_before(tmp_node->value.list, arr[i]);
        }
    }
    tmp_node->next = init(tmp_node->next);
    printf("OK\n");
}

void right_push(db* nosqldb, char* key, char** arr, int len){
    db *tmp_node = nosqldb;

    if(len == 0){
        printf("wrong number of arguments\n");
        return;
    }
    // printf("len=%d\n", len);
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{ // update
            if(tmp_node->value_type != TYPE_LIST){
                printf("type error\n");
            }
            else if(tmp_node->value.list){
                LIST* last_list_node = tmp_node->value.list->pre;

                // prepare new value node
                for(int i=0;i<len;i++){
                    last_list_node = insert_list_value_after(last_list_node, arr[i]);
                }
                printf("OK\n");
            }
            else{
                LIST* last_list_node = tmp_node->value.list;
                for(int i=0;i<len;i++){
                    if(last_list_node == NULL){
                        tmp_node->value_type = TYPE_LIST;
                        tmp_node->value.list = init_list(tmp_node->value.list, arr[i]);
                        last_list_node = tmp_node->value.list;
                    }
                    else{
                        last_list_node = insert_list_value_after(last_list_node, arr[i]);
                    }
                }
                printf("OK\n");
            }
            return;
        }
    }
    // add new node
    tmp_node->key = (char*) malloc( strlen(key) );
    strcpy(tmp_node->key, key);

    LIST* last_list_node = tmp_node->value.list;
    for(int i=0;i<len;i++){
        LIST* new_list_node = (LIST*) malloc(sizeof(LIST));
        new_list_node->data = (char*) malloc(sizeof(char) * strlen(arr[i]));
        strcpy(new_list_node->data, arr[i]);
        if(last_list_node == NULL){
            tmp_node->value.list =  init_list(tmp_node->value.list, arr[i]);
            last_list_node = tmp_node->value.list;
        }
        else{
            last_list_node = insert_list_value_after(last_list_node, arr[i]);
        }
    }
        
    tmp_node->next = init(tmp_node->next);
    printf("OK\n");
}

char* left_pop(db * nosqldb, char* key){
    db * tmp_node = nosqldb;
    char* del_data;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{ // delete
            if(tmp_node->value_type != TYPE_LIST){
                printf("type error\n");
                return NULL;
            }
            if(tmp_node->value.list == NULL){
                printf("(NULL)\n");
                return NULL;
            }
            del_data = (char*) malloc(sizeof(char)*strlen(tmp_node->value.list->data));
            strcpy(del_data, tmp_node->value.list->data);
            if(list_len(nosqldb, key) == 1){
                free(tmp_node->value.list->data);
                free(tmp_node->value.list);
                tmp_node->value.list = NULL;
            }
            else{
                tmp_node->value.list->pre->next = tmp_node->value.list->next;
                tmp_node->value.list->next->pre = tmp_node->value.list->pre;
                LIST* delete_list_node = tmp_node->value.list;
                tmp_node->value.list = tmp_node->value.list->next;
                free(delete_list_node->data);
                free(delete_list_node);
            }
            return del_data;
        }
    }
    printf("key not found\n");
    return NULL;
}

char* right_pop(db * nosqldb, char* key){
    db * tmp_node = nosqldb;
    char* del_data;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{ // delete
            if(tmp_node->value_type != TYPE_LIST){
                printf("type error\n");
                return NULL;
            }
            if(tmp_node->value.list == NULL){
                printf("(NULL)\n");
                return NULL;
            }
            LIST* last_list_node = tmp_node->value.list->pre;
            del_data = (char*) malloc(sizeof(char)*strlen(last_list_node->data));
            strcpy(del_data, last_list_node->data);
            if(last_list_node == tmp_node->value.list){
                free(tmp_node->value.list->data);
                free(tmp_node->value.list);
                tmp_node->value.list = NULL;
            }
            else{
                last_list_node->pre->next = last_list_node->next;
                last_list_node->next->pre = last_list_node->pre;
                free(last_list_node->data);
                free(last_list_node);
            }
            return del_data;
        }
    }
    printf("key not found\n");
    return NULL;
}

void list_range(db* nosqldb, char* key, int start_idx, int end_idx){
    db *tmp_node = nosqldb;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node
            tmp_node = tmp_node->next;
        }
        else{
            if(tmp_node->value_type != TYPE_LIST){
                printf("type error\n");
            }
            else if(tmp_node->value.list){
                LIST* tmp_list_node = tmp_node->value.list;
                int len = list_len(nosqldb, key);
                for(int i=0;i<=end_idx; i++){
                    if(len-1 < i || (tmp_list_node==tmp_node->value.list && i>0)){
                        // printf("break?\n");
                        break;
                    }
                    if(i<start_idx){
                        tmp_list_node = tmp_list_node->next;
                        continue;
                    }
                    printf("%d) \"%s\"\n",i-start_idx+1, tmp_list_node->data);
                    tmp_list_node = tmp_list_node->next;
                }
                printf("\n");  
            }
            else{
                printf("list is empty\n");
            }
            return;
        }
        // printf("are you still here?\n");
    }
    printf("key not found\n");
}

db* init(db* my_nosql){
    my_nosql = (db*) malloc(sizeof(db));
    my_nosql->key = NULL;
    my_nosql->next = NULL;
    my_nosql->value.list = NULL;
    my_nosql->value.string = NULL;
    my_nosql->value.set = NULL;
    my_nosql->value_type = TYPE_NONE;
    return my_nosql;
}

void add_or_update_data(db * nosqldb, char* key, char* value){
    db *tmp_node = nosqldb;
    char* realloc_ret = NULL;
    
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node !!!!!len不一樣比較
            tmp_node = tmp_node->next;
        }
        else{ // update
            if(tmp_node->value_type == TYPE_STRING){
                realloc_ret = realloc(tmp_node->value.string, strlen(value)*sizeof(char));
                if(realloc_ret == NULL){
                    printf("fail to update\n");
                    return;
                }
                strcpy(tmp_node->value.string, value);
            }
            else{
                printf("type error\n");
                return;
            }
            printf("OK\n");
            return;
        }
    }
    // add new node
    tmp_node->key = (char*) malloc( strlen(key) );
    strcpy(tmp_node->key, key);
    tmp_node->value.string = (char*) malloc( strlen(value) );
    strcpy(tmp_node->value.string, value);
    tmp_node->value_type = TYPE_STRING;
    tmp_node->next = init(tmp_node->next);

    printf("OK\n");
}

int delete_data(db ** nosqldb_addr, char* key){
    db * nosqldb = *nosqldb_addr;
    db * tmp_node = nosqldb, * pre_node = NULL;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node !!!!!len不一樣比較
            pre_node = tmp_node;
            tmp_node = tmp_node->next;
        }
        else{ // delete
            if(pre_node==NULL){
                *nosqldb_addr = (*nosqldb_addr)->next;
            }
            else{
                pre_node->next = tmp_node->next;
            }
            free(tmp_node->key);
            // free(tmp_node->next);
            // free(tmp_node->value.string);
            free(tmp_node);
            return 1;
        }
    }
    return 0;
}

char* get_data(db * nosqldb, char* key){
    db * tmp_node = nosqldb;
    int find_key = 0;
    while(tmp_node->next != NULL) { //search every node to find whether have the same key existed
        if(strcmp(tmp_node->key, key)) { //search next node !!!!!len不一樣比較
            tmp_node = tmp_node->next;
        }
        else{
            find_key = 1;
            if(tmp_node->value_type == TYPE_STRING){
                return tmp_node->value.string;
            }
            else{
                printf("type error\n");
                break;
            }
        }
    }
    if(!find_key){
        printf("key not found\n");
    }
    char *result;
    result = NULL;
    return result;
}


// void show_all_data(db * nosqldb){
//     if(nosqldb->next == NULL){
//         printf("the database is empty!! \n\n");
//         return;
//     }

//     db * tmp_node = nosqldb;
//     int i=1;
//     while(tmp_node->next != NULL){
//         if(tmp_node->value.string){
//             printf("(%2d) %14s : %s \n", i, tmp_node->key, tmp_node->value.string);
//         }
//         tmp_node = tmp_node->next;
//         i++;
//     }
//     printf("\n");
// }