import asyncio
async def test_one(notifications_server):
    while True:
        await asyncio.sleep(1)
