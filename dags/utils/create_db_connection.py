from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Dict, Any, Tuple
import asyncio
from asyncio import Lock
from contextlib import asynccontextmanager

# 설정 정보를 공통으로 사용
DB_CONFIG = {
    'user': 'niscom',
    'password': 'niscom',
    'host': '116.37.91.221',
    'port': 3306,
    'database': 'baseball'
}

# 동기 DB 연결 싱글톤 클래스
class SyncDBConnectionManager:
    _instance = None
    _engine = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SyncDBConnectionManager, cls).__new__(cls)
            cls._instance._engine = None
        return cls._instance
    
    def initialize(self):
        if self._engine is None:
            self._engine = create_engine(
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )
        return self._engine
    
    def dispose(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = SyncDBConnectionManager()
        return cls._instance

# 비동기 DB 연결 싱글톤 클래스
class AsyncDBConnectionManager:
    _instance = None
    _engine = None
    _session_factory = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AsyncDBConnectionManager, cls).__new__(cls)
            cls._instance._engine = None
            cls._instance._session_factory = None
        return cls._instance
    
    async def initialize(self):
        # 여러 태스크가 동시에 초기화하는 것을 방지하기 위한 락 사용
        async with self._lock:
            if self._engine is None:
                self._engine = create_async_engine(
                    f"mysql+aiomysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
                    echo=False,
                    pool_size=20,
                    max_overflow=30,
                    pool_pre_ping=True,
                    pool_recycle=3600
                )
                
                self._session_factory = sessionmaker(
                    bind=self._engine,
                    expire_on_commit=False,
                    class_=AsyncSession
                )
        
        return self._engine, self._session_factory
    
    async def dispose(self):
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = AsyncDBConnectionManager()
        return cls._instance

# 편의성을 위한 함수들 - 기존 코드와 호환되는 인터페이스

def get_sync_db_connection():
    """동기식 DB 엔진을 가져오는 함수 (기존 코드와의 호환성 유지)"""
    manager = SyncDBConnectionManager.get_instance()
    return manager.initialize()

async def get_async_db_connection():
    """비동기식 DB 엔진과 세션 팩토리를 가져오는 함수 (기존 코드와의 호환성 유지)"""
    manager = AsyncDBConnectionManager.get_instance()
    return await manager.initialize()

# 컨텍스트 매니저를 사용한 DB 세션 관리

@contextmanager
def get_db_session():
    """동기식 DB 세션을 컨텍스트 매니저로 제공"""
    engine = get_sync_db_connection()
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()

@asynccontextmanager
async def get_async_db_session():
    """비동기식 DB 세션을 컨텍스트 매니저로 제공"""
    manager = AsyncDBConnectionManager.get_instance()
    _, session_factory = await manager.initialize()
    async_session = session_factory()
    try:
        yield async_session
    finally:
        await async_session.close()

# 사용 예시:
# 동기 방식
# with get_db_session() as session:
#     result = session.execute("SELECT * FROM some_table")
#     session.commit()

# 비동기 방식
# async with get_async_db_session() as session:
#     result = await session.execute("SELECT * FROM some_table")
#     await session.commit()