import logging
import asyncio
import aiomysql


def log(sql, args=()):
    logging.info('''SQL: %s
        args: ''' % sql, args)


async def create_pool(loop, **kw):
    """
       创建全局连接池，每个HTTP请求都可以从连接池中直接获取数据库连接。使用连接池的好处
    是不必频繁地打开和关闭数据库连接，而是能复用就尽量复用。
    """
    logging.info('create database connection pool...')
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


async def select(sql, args, size=None):
    """
        封装SELECT语句，需要传入SQL语句和SQL参数。
        SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换。注意要始终
    使用带参数的SQL，而不是自己拼接SQL字符串，这样可以防止SQL注入攻击。
        fetchmany()获取最多指定数量的记录，fetch()获取所有记录。
    """
    log(sql, args)
    global __pool
    async with __pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs


async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.acquire() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected


def create_args_string(num):
    return ','.join(['?'] * num)


class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name  # 字段名
        self.column_type = column_type  # 字段类型
        self.primary_key = primary_key  # 是否主键
        self.default = default  # 默认值

    def __str__(self):
        return '<%s, %s: %s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    """
        字符串类型字段，数据类型默认为varchar(255)
    """

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(255)'):
        super().__init__(name, ddl, primary_key, default)


class IntegerField(Field):
    """
        整数类型字段，数据类型默认为bigint
    """

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    """
        浮点数类型字段，数据类型默认为double
    """

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class DateTimeField(Field):
    """
        时间类型字段，数据类型默认为datetime
    """

    def __init__(self, name=None, default=None):
        super().__init__(name, 'datetime', False, default)


class TextField(Field):
    """
        文本类型字段，数据类型默认为text
    """

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class BooleanField(Field):
    """
        布尔类型字段，数据类型默认为False
    """

    def __init__(self, name=None, default=None):
        super().__init__(name, 'boolean', False, default)


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise RuntimeError(
                            'Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields = list(map(lambda f: '`%s`' %
                                  mappings.get(f).name or f, fields))
        pk = mappings.get(primaryKey).name or primaryKey
        attrs['__mappings__'] = mappings  # 保存属性到列的映射关系
        attrs['__table__'] = tableName  # 保存表名
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 保存除主键外的属性名
        # 构造默认的SELECT，INSERT，UPDATE和DELETE语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (
            pk, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (`%s`, %s) values(%s)' % (
            tableName, pk, ','.join(escaped_fields), create_args_string(len(fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (
            tableName, ','.join(map(lambda f: '%s=?' % f, escaped_fields)), pk)
        attrs['__delete__'] = 'delete from `%s` where `%s` = ?' % (
            tableName, pk)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super().__init__(**kw)

    def __getattr__(self, key):
        """
            实现此方法，可使用对象属性方式访问dict。
        """
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        """
            实现此方法，可使用对象属性方法设置dict。
        """
        self[key] = value

    def getValue(self, key):
        """
            根据传入的键获取对应的值，如果不存在则返回None。
        """
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        """
            根据传入的键获取对应的值，如果键不存在则判断是否有设置默认值，有默认值则
        返回默认值，并设置该字段。
        """
        value = getattr(self, key, None)
        if value is None:
            field = self.__mapping__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' %
                              (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    async def find(cls, pk):
        ' 通过主键查询'
        rs = await select('%s where `%s` = ?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' 通过where查询'
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where.'
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    async def save(self):
        ' 插入数据到数据库'
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.insert(0, self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('faild to insert record: affected rows: %s' % rows)

    async def update(self):
        ' 更新数据到数据库'
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn(
                'failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        ' 从数据库删除记录'
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn(
                'failed to remove by primary key: affected rows: %s' % rows)


class User(Model):
    __table__ = 'user'

    id = IntegerField('id', primary_key=True)
    name = StringField('name')


async def fun(loop):
    await create_pool(loop, user='root', password='111111', db='test')
    n = await User.find(pk=3)
    await n.remove()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fun(loop))
    loop.run_forever()
