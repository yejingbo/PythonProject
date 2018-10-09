#!/usr/bin/env python3
#-*- coding:utf-8 -*-

__auther__='yejingbo'

import asyncio,logging
import aiomysql

def log(sql,args=()):
    logging.info('SQL:%s'%sql)
#创建数据库连接池
async def creat_pool(loop,**kw):
    logging.info('create datatase connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )
#定义select方法
async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cousor:
            await cousor.excute(sql.replace('?','%s'),args or ())
            if size:
                rs=await cousor.fetchmany(size)
            else:
                rs=await cousor.fetchall()
        logging.info('rows returned:%s'%len(rs))
        return rs
#定义其他方法
async def excute(sql,args,autocommit=True):
    log(sql)
    global __pool
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cousor:
                await cousor.excute(sql.replace('?','%s'),args)
                affected=cousor.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

def create_arg_string(num):
    l=[]
    for n in range(num):
        l.append('?')
    return ','.join(l)

class Field():
    def __init__(self,name,column_type,primary_key,default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default
    def __str__(self):
        return '<%s:%s:%s>'%(self.__class__.__name__,self.column_type,self.name)

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

class ModelMetaClass(type):
    def __new__(cls, name, bases,attrs):
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        tableName=attrs.get('__table__',None) or name
        logging.info('found model:%s (table:%s)'%(name,tableName))
        mappings=dict()
        fields=[]
        primary_key=None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info('found mapping:%s==>%s',(k,v))
                mappings[k]=v
                if v.primary_key==True:
                    if primary_key:
                        raise StandardError ('Duplicate primary key for field: %s' % k)
                    primary_key=k
                else:
                    fields.append(k)
        if not primary_key:
            raise StandardError('primary key not found')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
        tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
        tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict,metaclass=ModelMetaClass):
    def __init__(self,**kws):
        super(Model,self).__init__(**kws)

    def __getattr__(self, key):
        try:
            return self.get(key)
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key]=value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value==None:
            field=self.__mappings__[key]
            if field.default is not None:
                value=field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self,key,value)
        return valuedtu













user=User(id=1,name='yejingbo')
assert user.__select__=='select `id`, `name` from `User`','Bad select result'
assert user.__insert__=='insert into `User` (`name`, `id`) values (?, ?)','Bad insert result'
assert user.__update__=='update `User` set `name`=? where `id`=?','Bad update result'
