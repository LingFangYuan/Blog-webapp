import orm
import asyncio
import logging
from models import User, Blog, Comment

logging.basicConfig(level=logging.DEBUG)


async def test(loop):
    await orm.create_pool(loop=loop, user='root', password='111111', db='blogs')
    u = User(name='Test', email='test@exmple.com',
             passwd='123456', image='about:blank')
    await u.save()
    print(u.name)
    # u = await User.findNumber(selectField='count(*)')
    # print(u)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.run_forever()
