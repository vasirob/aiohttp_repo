from datetime import datetime
import asyncio

from aiohttp import web


routes = web.RouteTableDef()


async def arithmetic_progression(num, delta, a_0, interval):
    dt_start = datetime.utcnow()
    members = []
    for i in range(1, num + 1):
        a_i = a_0 + delta * (i - 1) * delta
        members.append(a_i)
        chunk = dict(Num=num, a_0=a_0, delta=delta, interval=interval,
                     dt_start=dt_start.isoformat(sep=' ', timespec='seconds'))
        yield chunk
        print('counting:', interval)
        await asyncio.sleep(interval)

    print('Task done:', members)


@routes.get('/add_task')
async def add_task(request):
    try:
        kwargs = {k: int(v) for k, v in request.query.items()}
    except ValueError:
        raise web.HTTPBadRequest() from ValueError('Error: bad input')

    queue.put_nowait((arithmetic_progression(**kwargs), kwargs))
    result = {'status': 'ok'}
    return web.json_response(result)


@routes.get('/queues')
async def get_queues_info(request):
    q = queue
    profiling_queue = asyncio.Queue(10)
    i = 0
    tasks = []
    while q.empty() is False:
        task, metadata = await q.get()
        tasks.append({
            **metadata,
            'position': i,
            'status': 'enqueued'
        })
        await profiling_queue.put((task, metadata))
        i += 1

    while profiling_queue.empty() is False:
        await q.put(await profiling_queue.get())

    result = {
        'queue_size': q.qsize(),
        'cur_frame': cur_frame,
        'tasks_in_queue': tasks
    }
    return web.json_response(result)


queue = asyncio.Queue(10)
cur_frame = None


async def task_worker(app):
    q = queue
    while True:
        if q.qsize() == 0:
            await asyncio.sleep(5)

        global cur_frame
        worker, _ = await q.get()
        async for chunk in worker:
            cur_frame = chunk
        cur_frame = None


async def tasks_start(app):
    app['task_queue'] = asyncio.create_task(task_worker(app))


async def tasks_clear(app):
    app['task_queue'].cancel()
    await app['task_queue']


app = web.Application()
app.add_routes(routes)
app.on_startup.append(tasks_start)
app.on_cleanup.append(tasks_clear)


if __name__ == '__main__':
    web.run_app(app)
