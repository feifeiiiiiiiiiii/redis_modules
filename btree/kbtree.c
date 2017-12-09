/*************************************************************************
	> File Name: kbtree.c
	> Author: feifeiiiiiiiiiii
	> Mail: qinpengfei.2008@gmail.com
	> Created Time: Wed 06 Dec 2017 07:31:07 PM PST
 ************************************************************************/

#include <stdio.h>
#include "kbtree.h"
#include "redismodule.h"

typedef struct {
    uint32_t key;
    uint32_t value;
} elem;

#define redismodule_cmp(a, b) (((b).key < (a).key) - ((a).key < (b).key))

KBTREE_INIT(redismodule_btree, elem, redismodule_cmp)

static RedisModuleType *BtreeType;

struct BtreeObject {
    kbtree_t(redismodule_btree) *b;
};

static struct BtreeObject *createBtreeObject(void) {
    struct BtreeObject *o;
    o = RedisModule_Alloc(sizeof(*o));
    o->b = kb_init(redismodule_btree, KB_DEFAULT_SIZE);
    return o;
}

/* BTREE_1_1.SET key field value */
int BtreeTypeSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != BtreeType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    struct BtreeObject *bto;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_Log(ctx, "warning","init btree");
        bto = createBtreeObject();
        RedisModule_ModuleTypeSetValue(key,BtreeType, bto);
    } else {
        bto = RedisModule_ModuleTypeGetValue(key);
    }

    long long field, value;
    if ((RedisModule_StringToLongLong(argv[2],&field) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    if ((RedisModule_StringToLongLong(argv[3],&value) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    elem e;
    e.key = field;
    e.value = value;
    //e.value = RedisModule_CreateStringFromString(ctx, argv[3]);

    if(kb_getp(redismodule_btree, bto->b, &e) != NULL) {
        return RedisModule_ReplyWithError(ctx, "-ERR field exist");
    }

    RedisModule_Log(ctx, "warning","put value");
    kb_putp(redismodule_btree, bto->b, &e);

    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}

/* BTREE_1_1.get key field */
int BtreeTypeGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != BtreeType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long long value;
    if ((RedisModule_StringToLongLong(argv[2], &value) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }
    
    struct BtreeObject *bto;
    bto = RedisModule_ModuleTypeGetValue(key);

    if(bto == NULL) {
        return RedisModule_ReplyWithError(ctx, "-ERR not found");
    }

    elem e, *p;
    e.key = value;

    p = kb_getp(redismodule_btree, bto->b, &e);
    if(p == NULL) {
        return RedisModule_ReplyWithError(ctx, "-ERR not found");
    }
    RedisModule_ReplyWithLongLong(ctx, p->value);

    return REDISMODULE_OK;
}


/* ========================== "kbtreetype" type methods ======================= */

void *BtreeTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        RedisModule_LogIOError(rdb, "warning","Can't load data with version %d", encver);
        return NULL;
    }

    struct BtreeObject *bto = createBtreeObject();
    int size = 0;
    uint64_t elements = RedisModule_LoadUnsigned(rdb);
    while(elements--) {
        long long key, value;
        key = RedisModule_LoadSigned(rdb);
        value = RedisModule_LoadSigned(rdb);
        elem e;
        e.key = key;
        e.value = value;
        kb_putp(redismodule_btree, bto->b, &e);
    }
    return bto;
}

void BtreeTypeRdbSave(RedisModuleIO *rdb, void *value) {
    struct BtreeObject *bto = value;
    kbitr_t itr;
    elem *p;

    RedisModule_SaveUnsigned(rdb, kb_size(bto->b));
    kb_itr_first(redismodule_btree, bto->b, &itr); // get an iterator pointing to the first
    for (; kb_itr_valid(&itr); kb_itr_next(redismodule_btree, bto->b, &itr)) { // move on
        p = &kb_itr_key(elem, &itr);
        //RedisModule_SaveUnsigned(rdb, 2);
        RedisModule_SaveSigned(rdb, p->key);
        RedisModule_SaveSigned(rdb, p->value);
    }
}

void BtreeTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
}

/* The goal of this function is to return the amount of memory used by
 * the HelloType value. */
size_t BtreeTypeMemUsage(const void *value) {
    return 0;
}

void BtreeTypeFree(void *value) {
}

void BtreeTypeDigest(RedisModuleDigest *md, void *value) {
}


/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"btree_1_1",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = BtreeTypeRdbLoad,
        .rdb_save = BtreeTypeRdbSave,
        .aof_rewrite = BtreeTypeAofRewrite,
        .mem_usage = BtreeTypeMemUsage,
        .free = BtreeTypeFree,
        .digest = BtreeTypeDigest 
    };

    BtreeType = RedisModule_CreateDataType(ctx,"btree_1_1",0,&tm);
    if (BtreeType == NULL) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"btree_1_1.set",
        BtreeTypeSet_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"btree_1_1.get",
        BtreeTypeGet_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;


    return REDISMODULE_OK;
}
