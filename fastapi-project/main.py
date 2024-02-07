import uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi_pagination import add_pagination
from redis.asyncio import Redis

from src.api.v1 import films, genres, persons
from src.core import config
from src.db import _redis
from src.db.elastic import elastic

app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    description='Information regarding films, genres and people who took part in the films creation',
    version='1.0.0'
)


@app.on_event('startup')
async def startup():
    _redis.redis = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
    elastic.es = AsyncElasticsearch(hosts=[f'{config.ES_HOST}:{config.ES_PORT}'])


@app.on_event('shutdown')
async def shutdown():
    await _redis.redis.close()
    await elastic.es.close()


app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(genres.router, prefix='/api/v1/genres', tags=['genres'])
app.include_router(persons.router, prefix='/api/v1/persons', tags=['persons'])

add_pagination(app)

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
    )
