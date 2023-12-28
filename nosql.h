#ifndef INIT_HASH_TABLE_SIZE
#define INIT_HASH_TABLE_SIZE 10
#endif

#ifndef NOSQL_H
#define NOSQL_H

typedef struct list
{
    char * data;
    struct list * next;
    struct list * pre;
} LIST;

typedef struct set
{
    char * data;
    int priority;
    struct set * next;
    struct set * pre;
} SET;

typedef struct hash_node
{
    char* field;
    char * value;
    struct hash_node * next;
} HASH_NODE;

typedef struct hash_table
{
    HASH_NODE** table;
    int table_size;
    int num_node;
} HASH_TABLE;


typedef union value
{
    char * string;
    LIST * list;
    SET * set;
    HASH_TABLE * hash_table;
    // int type;
} VALUE;

typedef struct database {
    char * key;
    int value_type; //1:string 2:list 3:set 4:hash_table 5:none
    VALUE value;
    struct database * next;
} db;

// int hash_table_size = INIT_HASH_TABLE_SIZE;

db* init(db* my_nosql);
void add_or_update_data(db * nosqldb, char* key, char* value);
// void show_all_data(db * nosqldb);
int delete_data(db ** nosqldb_addr, char* key);
char* get_data(db * nosqldb, char* key);
int list_len(db* nosqldb, char* key);
void left_push(db* nosqldb, char* key, char** arr, int len);
void right_push(db* nosqldb, char* key, char** arr, int len);
char* left_pop(db * nosqldb_addr, char* key);
char* right_pop(db * nosqldb_addr, char* key);
void list_range(db* nosqldb, char* key, int start_idx, int end_idx);
void set_add(db* nosqldb, char* key, char** arr, int* arr_priority, int len);
int set_card(db* nosqldb, char* key);
int set_count(db* nosqldb, char* key, int min, int max);
void set_range(db* nosqldb, char* key, int start_idx, int end_idx, int withscore);
void set_range_by_score(db* nosqldb, char* key, int min, int max, int withscore);
int set_rank(db* nosqldb, char* key, char* data);
int set_remove(db* nosqldb, char* key, char** arr, int len);
int set_remove_range_by_score(db* nosqldb, char* key, int min, int max);
int set_interstore(db* nosqldb, char* dst_key, int numkeys, char** keys, int* weights);
int set_unionstore(db* nosqldb, char* dst_key, int numkeys, char** keys, int* weights);
int hash_set(db* nosqldb, char* hash_table_name /*node的key*/, char* field, char* value);
char* hash_get(db* nosqldb, char* hash_table_name /*node的key*/, char* field);
int hash_del(db* nosqldb, char* hash_table_name, char* field);

#endif