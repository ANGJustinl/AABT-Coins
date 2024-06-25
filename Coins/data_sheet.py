"""数据库操作模块, 用于存储用户Coins, 以及群聊是否允许签到, 以及每天的收获量"""
import os
import random
import time
from typing import Dict, List

from sqlalchemy import (
    Boolean,
    Column,
    Engine,
    Float,
    Integer,
    String,
    create_engine,
    orm,
)
from sqlalchemy.orm import sessionmaker

# 数据库路径
DATA_PATH = "data/Coins"

# 不存在则创建文件夹
if not os.path.exists("data"):
    os.mkdir("data")
if not os.path.exists(DATA_PATH):
    os.mkdir(DATA_PATH)


engine: Engine = create_engine(f"sqlite:///{DATA_PATH}/Coin.db")
session = sessionmaker(engine)
Base = orm.declarative_base()


class UserData(Base):
    """用户数据表"""

    __tablename__: str = "userdata"

    userid = Column(Integer, primary_key=True, index=True)
    Coins = Column(Float, nullable=False)
    last_login = Column(Integer, nullable=False, default=0)


class GroupData(Base):
    """群数据表"""

    __tablename__: str = "groupdata"

    groupid = Column(Integer, primary_key=True, index=True)
    allow = Column(Boolean, nullable=False)


class PayData(Base):
    """转账数据表"""

    __tablename__: str = "pay_data"

    id = Column(Integer, primary_key=True)
    userid = Column(Integer, nullable=False, index=True)
    date = Column(String(20), nullable=False)
    volume = Column(Integer, nullable=False)


Base.metadata.create_all(engine)


def is_in_table(userid: int) -> bool:
    """传入一个userid，判断是否在表中"""
    with session() as s:
        return bool(s.query(UserData).filter(UserData.userid == userid).first())


def add_new_user(userid: int) -> None:
    """插入一个新用户, 默认Coins是10.0"""
    with session() as s:
        s.add(
            UserData(
                userid=userid, Coins=10.0, last_login=int(time.time())
            )
        )
        s.commit()


def update_activity(userid: int) -> None:
    """更新用户活跃时间"""
    # 如果用户不在表中, 则插入一条记录
    if not is_in_table(userid):
        add_new_user(userid)
    with session() as s:
        s.query(UserData).filter(UserData.userid == userid).update(
            {UserData.last_login: int(time.time())}
        )
        s.commit()


def get_Coins(userid: int) -> float:
    """传入用户id, 返还数据库中对应的Coins"""
    with session() as s:
        return s.query(UserData).filter(UserData.userid == userid).first().Coins  # type: ignore


def set_Coins(userid: int, length: float) -> None:
    """传入一个用户id以及需要增加的长度, 在数据库内累加, 用这个函数前一定要先判断用户是否在表中"""
    with session() as s:
        # 先获取当前的Coins, 然后再累加
        current_length = (
            s.query(UserData).filter(UserData.userid == userid).first().Coins
        )  # type: ignore
        s.query(UserData).filter(UserData.userid == userid).update(
            {
                UserData.Coins: round(current_length + length, 3),
                UserData.last_login: int(time.time()),
            }
        )
        s.commit()


def check_group_allow(groupid: int) -> bool:
    """检查群是否允许, 传入群号, 类型是int"""
    with session() as s:
        if s.query(GroupData).filter(GroupData.groupid == groupid).first():
            return s.query(GroupData).filter(GroupData.groupid == groupid).first().allow  # type: ignore
        else:
            return False


def set_group_allow(groupid: int, allow: bool) -> None:
    """设置群聊开启或者禁止签到, 传入群号, 类型是int, 以及allow, 类型是bool"""
    with session() as s:
        # 如果群号不存在, 则插入一条记录, 默认是禁止
        if not s.query(GroupData).filter(GroupData.groupid == groupid).first():
            s.add(GroupData(groupid=groupid, allow=False))
        # 然后再根据传入的allow来更新
        s.query(GroupData).filter(GroupData.groupid == groupid).update(
            {GroupData.allow: allow}
        )
        s.commit()


def get_today() -> str:
    """获取当前年月日格式: 2023-02-04"""
    return time.strftime("%Y-%m-%d", time.localtime())


def insert_pay(userid: int, volume: float) -> None:
    """插入一条记录"""
    volume = int(volume)
    now_date = get_today()
    with session() as s:
        # 如果没有这个用户的记录, 则插入一条
        if (
            not s.query(PayData)
            .filter(PayData.userid == userid)
            .first()
        ):
            s.add(PayData(userid=userid, date=now_date, volume=volume))
        # 如果有这个用户以及这一天的记录, 则累加
        elif (
            s.query(PayData)
            .filter(PayData.userid == userid, PayData.date == now_date)
            .first()
        ):
            # 当前的值
            current_volume = (
                s.query(PayData)
                .filter(
                    PayData.userid == userid, PayData.date == now_date
                )
                .first()
                .volume
            )  # type: ignore
            s.query(PayData).filter(
                PayData.userid == userid, PayData.date == now_date
            ).update({PayData.volume: round(current_volume + volume, 3)})
        # 如果有这个用户但是没有这一天的记录, 则插入一条
        else:
            s.add(PayData(userid=userid, date=now_date, volume=volume))
        s.commit()


def get_pay_data(userid: int) -> List[Dict]:
    """获取一个用户的所有转账记录"""
    with session() as s:
        return [
            {"date": i.date, "volume": i.volume}
            for i in s.query(PayData).filter(PayData.userid == userid)
        ]


def get_today_pay_data(userid: int) -> float:
    """获取用户当日的转账金额"""
    with session() as s:
        # 如果找得到这个用户的id以及今天的日期, 则返回注入量
        if (
            s.query(PayData)
            .filter(
                PayData.userid == userid, PayData.date == get_today()
            )
            .first()
        ):
            return (
                s.query(PayData)
                .filter(
                    PayData.userid == userid,
                    PayData.date == get_today(),
                )
                .first()
                .volume
            )  # type: ignore
        # 否则返回0
        else:
            return 0.0


def punish_all_inactive_users() -> None:
    """所有不活跃的用户, 即上次签到, 所有Coins大于1将受到减少0--1随机的惩罚"""
    with session() as s:
        for i in s.query(UserData).all():
            if time.time() - i.last_login > 86400 and i.Coins > 1:
                i.Coins = round(i.Coins - random.random(), 3)
        s.commit()


def get_sorted() -> List[Dict]:
    """获取所有用户的Coins, 并且按照从大到小排序"""
    with session() as s:
        return [
            {"userid": i.userid, "Coins": i.Coins}
            for i in s.query(UserData).order_by(UserData.Coins.desc())
        ]
