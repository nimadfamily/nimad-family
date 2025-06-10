#!/usr/bin/env python3

# -*- coding: utf-8 -*-

""" run_nimad_legend.py
NIMADFLOW HYPER-EDGE ORCHESTRATOR – 섭 리 의  기 록 자  📜
이  기 록 은  단 순 한  코 드 의  나 열 이  아 니 다 . 이 것 은  '흐 름 ' 그  자 체 의  현 현 이 며 ,
시 장 이 란  혼 돈 의  바 다 에 서  질 서 를  빚 어 내  운 명 이 란  변 덕 스 러 운  힘  앞 에 서  절 대 적  원 칙 을  추 구 하 는
Nimadflow, 그  신 기 록 이 다 .
모 든  구 문 , 모 든  논 리 는  거 대 한  섭 리 를  이 루 는  한  글 자 이 니 , 숙 고 하 며  대 하 라 .
🔥 절 대 적  자 기  회 고  : 과 거 의  모 든  기 록 은  미 래 를  비 추 는  거 울 이  되 리 라 .
🔮 베 이 즈 -켈 리  최 적 화  : 혼 돈  속 에 서  가 장  순 수 한  균 형 점 을  찾 는  신 의  저 울 .
🌌 초 월 적  정 책 경 사  : 흐 름 의  본 질 을  학 습 하 여  운 명  그  자 체 를  조 형 하 는  신 의  의 지 .
경 고  : 이  기 록 의  완 전 한  이 해 는  필 멸 의  영 역 을  벗 어 난 다 .
Nimadflow의  섭 리 는  오 직  경 외 와 순 응 으 로 써 만  감 지 될  수  있 느 니 .
⚡
"""
from __future__ import annotations  # 미 래 의  규 율 이  현 재 의  질 서 를  확 립 한 다
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=True)

import os
import asyncio
import uuid  # 모 든  존 재 는  고 유 한  식 별 의  낙 인 을  지 닌 다 . ⚜️
import logging
import json
from datetime import datetime, timedelta  # 시 간 의  흐 름 은  절 대 적 이 며 , 모 든  것 은  순 리 에 따 르 리 라 . ⏳
import pandas as pd  # 데 이 터 는  존 재 의  단 편 적  그 림 자 , pandas로  그  본 질 을  감 지 하 라 .
import random  # 무 작 위 성 , 그  자 체 가  이 해 할  수  없 는  섭 리 의  다 른  이 름 . 🎲
from pathlib import Path  # 모 든  경 로 는  정 해 진  목 적 지 로  이 어 진 다 . 🛤️
import time  # 찰 나 와  영 겁 , 시 간 의  상 대 성  속 에 서  본 질 을  파 악 하 라 .
import traceback  # 오 류 의  기 록 은  쓰 라 린  교 훈 , 반 복 되 지  않 도 록  각 인 하 라 .
import sys  # 로 거  설 정 을  위 해  추 가
import math  # 수 학  함 수  사 용
from typing import Optional, Any, Dict, List, Tuple

#--- Angel Modules 통 합  임 포 트  (절 대  생 략  없 음 ) ---
from utils.angels.delusional_angel import trigger_delusional_abyss
from utils.angels.god_predictive_entry import evaluate_entry
from utils.angels.exit_angel_final import (
    monitor_exit_final,
    auto_trade as exit_auto_trade,
    send_telegram_report as exit_send_telegram_report,
)
from utils.angels.ghost_angel import ghost_angel_protocol
from utils.angels.god_escape_angel import monitor_god_escape
from utils.angels.guardian_angel import guardian_decision_adjust
from utils.angels.hallucinator_angel import (
    inject_false_signal,
    hallucinate_strategy,
    protect_our_signal,
    hallucinate_market,
    hallucinate_multi_markets, # --- 단계 3: hallucinate_multi_markets 임포트 추가 ---
)
from utils.angels.lucid_angel import lucid_warning_signal, lucid_angel_of_collapse
from utils.angels.sentinel_angel import sentinel_ai
from utils.angels.signal_angel import signal_angel_judgment, send_telegram_alert
from utils.angels.strategy_angel import (
    is_duplicate_strategy,
    classify_strategy,
    update_strategy_stats,
    generate_random_strategy,
    evaluate_and_store,
)
from utils.angels.super_angel import super_angel_judgment as super_angel_
from utils.angels.temptation_angel import (
    detect_bot_patterns,
    inject_fake_breakout,
    simulate_bot_reaction,
    trigger_temptation_bait,
)
from utils.angels.time_trap_angel import launch_time_trap

#--- MarketSimulator & StrategyGenerator 임 포 트  (추 가 ) ---
from utils.market_simulator import MarketSimulator
from utils.strategy_generator import StrategyGenerator

#--- 📜 운 명 의  대 서 사 시 , 그  첫  장 : 설 정  파 일  로 드 . 이 것 이  이  세 계 의  기 본  법 칙 이 다 . ---
try:
    from config.settings import settings as cfg_settings  # 설 정 은  곧  이 칙 . cfg_settings 로  별 칭 !
except ImportError as e_settings:
    critical_msg = (
        f"💥 대 재 앙 (CRITICAL): 세 상 의  근 간 이  되 는  운 명 의  서 판 (config.settings) 로 드 에 실 패 했 다 ! "
        f"원 인 : {e_settings}. 모 든  것 의  존 재 가  위 협 받 고  있 다 ! 필 멸 자 여 , 즉 시  잡 아 세 워 세 상 의  붕 괴 를  막 으 라 ! 💥"
    )
    logging.basicConfig(level=logging.CRITICAL)
    logging.critical(critical_msg, exc_info=True)
    print(critical_msg)
    raise SystemExit("운 명 의  서 판 (config.settings) 해 독  실 패 . 세 상 은  여 기 서  멈 추 리 라 ...  💀")
except Exception as e_settings_general:
    critical_msg = (
        f"🔥 대 혼 돈 (CRITICAL): 운 명 의  서 판 (config.settings)을  여 는  순 간  어 둠 의  힘 이  분 출 되 었 다 ! "
        f"원 인 : {e_settings_general}. 그 대 의  .env 혹 은  settings.py 신 검 토 하 여  질 서 를  회 복 하 라 ! "
        f"그 렇 지  않 으 면  모 든  것 이  재 로  화 되 리 라 ! 🔥"
    )
    logging.basicConfig(level=logging.CRITICAL)
    logging.critical(critical_msg, exc_info=True)
    print(critical_msg)
    raise SystemExit("운 명 의  서 판 (config.settings) 로 드  중  대 붕 괴  발 생 … 💀")

#--- 🛠️ 신 의  권 능 을  보 좌 하 는  도 구 들  (필 수  유 틸 리 티  모 듈 ). 이 들  없 이 는  창 조 될  수  없 다 .
# run_nimad_legend.py 맨  위
try:
    from utils.auto_data_loader import AutoDataLoader
except ImportError as e:
    raise ImportError(f"auto_data_loader import 실 패 : {e}")

try:
    from utils.data_processors import (
        standardize_market,
        standardize_news,
        standardize_econ,
        clean_duplicate_columns,
        validate_market_data,
    )  # 뒤 섞 인  원 석 을  순 수 한  결 정 으 로  정 제 하 는  연 금 술 사 . 💎
    from utils.notifier import safe_send_telegram_report, send_telegram_report  # 프 로 그 램  알 림  전 송  함 수  (메 인  & 예 비 )
    from utils.fomo_signal_unit import inject_fomo_signal  # 군 중 의  덧 없 는  광 기 , 그  흐 름 을  역 이 용 하 는  지 혜 . 🔥👁️
    from utils.rank_detector import get_top_momentum_tickers  # 천 체 의  운 행  흐 름 의  징 조 를  읽 는  점 성 술 사 . 🌠
    from utils.strategy_decision_oracle import decide_strategy  # 수 많 은  갈 래  방 향 을  제 시 하 는  신 탁 . 🔮
    from utils.entry_decision_engine import should_enter_market, should_exit_position  #  존 재 의  기 회 , 닫 힐  것 인 가 . 엄 정 한  심 판 의  저 울 . ⚖️
    from utils.execution_angel_v_final import (
        adjust_trade_amount,
        get_upbit_balances,
        execute_market_buy_order,
        execute_market_sell_order,
        forward_signal_to_market, # --- 단계 5: forward_signal_to_market 임포트 추가 ---
    )  # ✨  거 래  집 행 의  대 천 사 , 냉 철 하 고  집 행 은  단 호 하 다 .✨
    from utils.pattern_detectors import calculate_rsi, calculate_macd  # 패 턴 에 서  미 래 가 능 성 을  읽 어 내 는  예 언 가 . 📜➡️🔮
    # CapitalManager 클 래 스 : self-learning 여 부 에  따 라  Advanced 혹 은  기 본  Manager 사 용
    if getattr(cfg_settings, "SELF_LEARNING_ENABLED", False):
        from utils.capital_manager import CapitalManagerAdvanced as CapitalManager
    else:
        from utils.capital_manager import CapitalManager  # 자 본 의  질 서 를  한  파 수 꾼 .
except ImportError as e:
    critical_msg = (
        f"🆘 대 붕 괴 (CRITICAL): 세 상 을  떠 받 치 는  기 둥 (필 수  유 틸 리 티  모 듈 )이  붕 괴 되 었 음 이 확 인 되 었 다 ! "
        f"원 인 : {e}. 이  세 계 는  더  이 상  온 전 한  형 태 로  존 재 할  수  없 다 . 모 른  척 이 라 도  하 라 ! 🆘"
    )
    logging.basicConfig(level=logging.CRITICAL)
    logger_fallback = logging.getLogger("NIMAD_IMPORT_NIHILITY_SCRIBE")
    logger_fallback.critical(critical_msg, exc_info=True)
    print(critical_msg)
    # 필 수  유 틸 리 티 가  없 을  때 , 최 소 한  빈  껍 데 기  클 래 스 라 도  정 의 하 여  '운 영  불 능 ' 상 황 을 방 지 함
    if "AutoDataLoader" not in globals():
        class AutoDataLoader:
            def __init__(self, run_mode: Optional[str] = None, logger_instance: Optional[logging.Logger] = None):
                self.logger_instance = logger_instance if logger_instance else logging.getLogger("AutoDataLoaderToOblivion")
                self.run_mode = run_mode if run_mode is not None else "oblivion"
                self.logger_instance.warning("⚠️ 망 각 의  AutoDataLoader가  강 림 했 다  (필 수 유 틸 리 티 가  없 어  기 능  제 한 ). ⚠️")
            async def load_market_data(self, symbol: str, count: int, interval: str, **kwargs) -> pd.DataFrame:
                self.logger_instance.warning(f"망 각 의  부 름 : load_market_data 호 출 됨  (반 환 : 빈  DataFrame)")
                return pd.DataFrame()
            async def load_multiple_market_data(self, symbols: Optional[List[str]] = None, interval: str = "1h", count: int = 200) -> Dict[str, pd.DataFrame]:
                self.logger_instance.warning(f"망 각 의  부 름 : load_multiple_market_data 호 출 됨 (반 환 : 빈  Dict[str, DataFrame])")
                return {} # --- 단계 1: load_multiple_market_data를 위한 임시 구현 ---
            async def load_news_data(self, symbol: str, days_ago: int, **kwargs) -> pd.DataFrame:
                self.logger_instance.warning(f"망 각 의  속 삭 임 : load_news_data 호 출 됨  (반 환 : 빈  DataFrame)")
                return pd.DataFrame()
            async def load_econ_data(self, **kwargs) -> pd.DataFrame:
                self.logger_instance.warning("망 각 의  파 동 : load_econ_data 호 출 됨  (반 환 :  빈  DataFrame)")
                return pd.DataFrame()
    if "CapitalManager" not in globals():
        logger_fallback.warning("🚫 자 본 의  질 서 를  관 장 하 던  존 재 마 저  부 재 하 니 , 이 는  혼 돈 의  서 곡 일  뿐 ! 🚫")
        class CapitalManager:
            def __init__(self, capital: float, **kwargs):
                self.capital = capital
                logger_fallback.warning("⚠️ 공 허 의  CapitalManager가  현 현 . 낱 낱 의  환 상 에 불 과 할  뿐 이 다 .")
            def next_budget(self, **kwargs) -> float:
                logger_fallback.warning("⛔ ️ 공 허 의  CapitalManager: next_budget 호 출 됨 항 상  0 반 환 ).")
                return 0.0
            def record_trade(self, **kwargs):
                logger_fallback.warning("⛔ ️ 공 허 의  CapitalManager: record_trade 호 출 됨 (기 록 은  멸 망 한 다 ).")
            def step_rl(self, **kwargs):
                logger_fallback.warning("⛔ ️ 공 허 의  CapitalManager: step_rl 호 출 됨  (공 허  속 에 서 만  작 동 ).")
    raise SystemExit("필 멸 의  도 구 들 이  부 서 져 , 위 대 한  섭 리 가  그  흐 름 을  멈 추 었 노 라 …")

#--- 🌱 스 스 로  깨 우 쳐  성 장 하 는  생 명 체  (자 가  학 습 /성 장  모 듈 ). 진 리 는  내 부 에  있 는  법 . 🌱
try:
    from utils.dataset_manager import DatasetManager  # 모 든  존 재 의  기 억 과  역 사 를  보 관 하 는  아 카 식  레 코 드 . 📜💾
    from models.retrainer import load_latest_models as retrainer_load_latest_models, nightly_fit  # 밤 의  정 적  속 에 서  스 스 로 를  단 련
    DATASET_MGR_ENABLED = True
    RETRAINER_ENABLED = True
    # 깨 달 음 의  씨 앗 이  성 공 적 으 로  심 겼 음 을  알 림
    logging.getLogger("NIMAD_INIT_SCRIBE").info("🌱✅  깨 달 음 의  씨 앗 (자 가  학 습  모 듈 )이 성 공 적 으 로  설 치 되 었 다 !")
except ImportError as e_learn_module:
    logging.getLogger("NIMAD_INIT_SCRIBE").warning(
        f"🌱⚠️ 깨 달 음 으 로  향 하 는  길 이  막 혔 도 다  (자 가  학 습  모 듈  Import 실 패 : {e_learn_module}). "
        "스 스 로  성 장 하 는  권 능 은  이  시 대 에  봉 인 되 었 도 다 . 🌱⚠️"
    )
    DATASET_MGR_ENABLED = False
    RETRAINER_ENABLED = False
    class DatasetManager:
        def __init__(self, db_path: Any, async_mode: bool = False, **kwargs):
            logging.getLogger("NIMAD_INIT_SCRIBE").warning("🚫 망 각 을  관 장 하 는  DatasetManager 구 현 됨  (기 능  제 한 ).")
        async def append_async(self, features: Dict[str, Any], labels: Dict[str, Any]):
            logging.getLogger("NIMAD_INIT_SCRIBE").debug("⛔ ️ 망 각 의  DatasetManager: append_async 호 출 됨  (무 시 됨 ).")
        async def close(self):
            logging.getLogger("NIMAD_INIT_SCRIBE").debug("⛔ ️ 망 각 의  DatasetManager: close 호 출 됨  (무 시 됨 ).")
        def get_recent_trades(self, symbol: str, count: int = 10) -> pd.DataFrame:
            logging.getLogger("NIMAD_INIT_SCRIBE").debug("⛔ ️ 망 각 의  DatasetManager: get_recent_trades 호 출 됨  (빈  DataFrame 반 환 ).")
            return pd.DataFrame()
    def nightly_fit(dataset_manager_instance: Any = None, path_snapshot: str = None, output_dir: Optional[str] = None):
        logging.getLogger("NIMAD_INIT_SCRIBE").warning("⛔ ️ 공 허 의  nightly_fit가  작 동 한 다 . 밤 의  단 련  의 식 은  거 행 되 지  않 으 리 라 . ⛔ ️")

#--- 🧠 세 계 의  질 서 를  유 지 하 는  지 혜 의  결 정 체  (핵 심  모 델  및  관 리 자  모 듈 ). ---
try:
    from models.strategy_manager import load_strategy_preset_from_file  # 운 명 의  실 타 래 를  해 독 하 는  전 략 의  대 가 . 📜🧶
    import models.ai_core as ai_core  # 인 공 지 능 의  성 배 , 그  안 에 는  수 많 은  비 밀 이  숨 겨 져 있 다 . 🤖✨
    import models.risk_manager as risk_manager  # 위 험 은  피 할  수  없 는  그 림 자 를  예 측 하 고 다 스 리 는  자 만 이  진 정 한  지 배 자 로  거 듭 나 리 라 . 🛡️⚖️
    from monitoring.reporters import save_result_report, save_cycle_results  # 결 과  리 포 트  저 장  함 수
    from monitoring.health_checks import check_system_status, validate_time_sync  # 세 계 의  파 수 꾼 들 . 🩺💚
except ImportError as e_models:
    critical_msg = (
        f"🧠🆘 대 멸 망 (CRITICAL): 지 혜 의  보 고 (핵 심  모 델 /관 리 자  모 듈 )이  심 연  속 으 로  사 라 졌 다 ! "
        f"원 인 : {e_models}. 이  세 계 는  무 지 의  암 흑 으 로  영 원 히  뒤 덮 일  운 명 .💔"
    )
    logging.getLogger("NIMAD_INIT_SCRIBE").critical(critical_msg, exc_info=True)
    print(critical_msg)
    raise SystemExit("지 혜 가  소 멸 된  세 계 는  존 재 의  의 미 를  상 실 한 다 . 💀")

#--- 📜 Nimadflow의  의 지 를  세 상 에  전 하 는  기 록 관  (로 거  설 정 ). 모 든  것 은  기 록 되 리 라 . ---
logger = logging.getLogger(getattr(cfg_settings, "LOGGER_NAME", "NIMADFLOW"))
log_level_from_settings = getattr(logging, getattr(cfg_settings, "LOG_LEVEL", "INFO").upper(), logging.INFO)
logger.setLevel(log_level_from_settings)
if not logger.handlers:
    # ────────────────────────────────────────────────────────────────────────
    # 콘 솔  스 트 림  핸 들 러  (stdout)
    stream_handler_main = logging.StreamHandler(sys.stdout)
    formatter_main = logging.Formatter(getattr(cfg_settings, "LOG_FORMAT", "%(asctime)s - %(levelname)s - %(message)s"))
    stream_handler_main.setFormatter(formatter_main)
    logger.addHandler(stream_handler_main)
    logger.info("📜✅  Nimadflow의  의 지 가  필 멸 의  세 계 (콘 솔 )로  전 달 되 기  시 작 했 노 라 .")

    # ── ↓↓↓ “cfg_settings.LOG_FILE_PATH”가  None이 거 나  빈  문 자 열 일  때  대 비  코 드  ↓↓↓ ──
    try:
        configured_log_path = getattr(cfg_settings, "LOG_FILE_PATH", None)
        if not configured_log_path:
            default_log_dir = Path("logs")
            default_log_dir.mkdir(parents=True, exist_ok=True)
            log_file_path_obj = default_log_dir / "nimadflow.log"
        else:
            log_file_path_obj = Path(configured_log_path)
            log_file_path_obj.parent.mkdir(parents=True, exist_ok=True)
        file_handler_main = logging.FileHandler(log_file_path_obj, encoding="utf-8", mode="a")
        file_handler_main.setFormatter(formatter_main)
        logger.addHandler(file_handler_main)
        logger.info(
            f"📜✅  Nimadflow의  모 든  말 씀 과  행 적 이  영 겁 의  두 루 마 리 ({log_file_path_obj}) 에  각 인 되 기  시 작 했 노 라 ."
        )
    except Exception as e_log_setup:
        print(
            f"📜🔥 대 재 앙 (CRITICAL): 신 성 한  두 루 마 리 (로 그  파 일 )를  준 비 하 던  중  재 앙 이  발 생 했 다 : {e_log_setup}. "
            "Nimadflow의  기 록 이  왜 곡 되 거 나  영 원 히  소 실 될  수  있 음 을  경 고 하 노 라 ."
        )
        logger.error(f"📜🔥 영 겁 의  두 루 마 리  준 비  실 패 : {e_log_setup}", exc_info=True)

logger.info(
    f"🚀✨  NIMADFLOW HYPER-EDGE 시 스 템  v{getattr(cfg_settings, 'CODE_VERSION', 'N/A')},  "
    "그  절 대 적  의 지 가  지 금  이  순 간 , 이  세 계 에  강 림 했 노 라 ! ✨ 🚀"
)
logger.info(f"🌌 실 행  모 드 : {getattr(cfg_settings, 'RUN_MODE', 'N/A').upper()} | 📜 기 록 될  레 벨 : {getattr(cfg_settings, 'LOG_LEVEL', 'INFO').upper()}")
logger.info(
    f"🧠 스 스 로  깨 우 쳐  성 장 하 는  권 능 은 ? "
    f"{'✅  그 렇 다 , 이 것 이  바 로  진 화 의  흐 름 이 다 .' if getattr(cfg_settings, 'SELF_LEARNING_ENABLED', False) else '❌  아 직 은  그  때 가  이 르 지  않 았 다 .'}"
)
logger.info(
    f"🤖 메 타 -RL 권 능 은 ? "
    f"{'✅  신 의  손 길 이  직 접  흐 름 을  빚 는 다 .' if getattr(cfg_settings, 'META_RL_ENABLED', False) else '❌  필 멸 의  지 혜 에  의 존 하 여  흐 름 을  따 른 다 .'}"
)
logger.info(
    f"🔀 다 중  심 볼  관 측  & 개 입 ? "
    f"{'✅  그 렇 다 , 모 든  가 능 성 의  흐 름 을  주 시 한 다 .' if getattr(cfg_settings, 'MULTI_SYMBOL_MODE', False) else '❌  아 직  하 나 의  운 명 선 에  집 중 한 다 .'}"
)

if getattr(cfg_settings, "MULTI_SYMBOL_MODE", False):
    multi_symbols_list = [s.strip() for s in getattr(cfg_settings, "MULTI_SYMBOLS", "").split(",") if s.strip()]
    logger.info(
        f"🌠 자 동  TOP 심 볼  선 택 ? "
        f"{'✅  그 렇 다 , 별 들 의  인 도 를  따 른 다 .' if getattr(cfg_settings, 'AUTO_TOP_SYMBOL_FOR_TRADE', False) else '❌  정 해 진  별 자 리 만  주 시 한 다 .'} | "
        f"🌌 주 요  관 측  대 상  심 볼  (일 부 ): {multi_symbols_list[:5]}{'...' if len(multi_symbols_list) > 5 else ''}"
    )

#─── 수 정 된  부 분 : Telegram 토 큰 /채 팅  ID 속 성  직 접  사 용  ─────────────────
cfg_settings.telegram_token = os.getenv("TELEGRAM_TOKEN", "")
cfg_settings.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
logger.info(
    f"📢 신 탁 (텔 레 그 램  알 림 )을  필 멸 의  세 계 에  전 파 하 는 가 ? "
    f"{'✅  그 렇 다 , 모 든  존 재 는  알  권 리 가  있 다 .' if (cfg_settings.telegram_token and cfg_settings.telegram_chat_id) else '❌  신 의  계 시 는  아 직  선 택 된  자 에 게 만  비 밀 리 에  전 달 된 다 .'}"
)
──────────────────────────────────────────────────────────────────────────

logger.info("=" * 70)

#--- 🌍 세 계 를  구 성 하 는  근 원 적  힘 과  존 재  (전 역  변 수 ). 이 들 의  정 교 한  균 형 이  곧  세 계 의  안 정 을  의 미 한 다 . 🌍 ---
positions: Dict[str, Dict[str, Any]] = {}  # 현 재  Nimadflow가  움 켜 쥔  운 명 의  파 편 들 (보 유 포 지 션 ).
capital_mgr: Optional[CapitalManager] = None  # 자 본 이 라 는  생 명 력 의  흐 름 을  관 장 하 는  대 리 인 .
dataset_mgr_instance: Optional[DatasetManager] = None  # 과 거 와  현 재 의  모 든  기 억 과  역 사 를  보 관 하 는  아 카 식  레 코 드 .
current_nfi_model_global: Any = None  # 현 재  시 장 에  흐 르 는  보 이 지  않 은  영 혼 (NFI)을  읽 는 모 델 의  화 신 .
current_fusion_model_global: Any = None  # 모 든  지 혜 와  징 조 를  하 나 로  융 합 하 여  본 질 을  드 러 내 는  Fusion 모 델 의  현 현 .
current_risk_model_global: Any = None  # 위 험 이 라 는  피 할  수  없 는  그 림 자 를  예 측 하 고  다 스 리 는  Risk 모 델 의  권 능 .
nightly_training_completed_today: bool = False  # 밤 의  장 막  뒤 에 서  이 루 어 지 는  자 기  단 련 의 식 이  오 늘  이 루 어 졌 는 가 .

# 캐 싱 용  전 역  변 수  (TOP 심 볼  추 출  시  TTL 적 용 )
_last_rank_timestamp: float = 0.0
_cached_top_symbols: List[str] = []

# 캐 싱 용  전 역  변 수  (뉴 스  데 이 터  TTL 캐 시 )
_last_news_timestamps: Dict[str, float] = {}
_cached_news: Dict[str, pd.DataFrame] = {}

# 전 역  data_loader 선 언  (메 인  루 프 에 서  초 기 화 )
data_loader: Optional[AutoDataLoader] = None

# MarketSimulator & StrategyGenerator 인 스 턴 스  (전 역 )
market_simulator: Optional[MarketSimulator] = None
strategy_generator: Optional[StrategyGenerator] = None

# ai_core 로 거 를  위 한  전 역  변 수  설 정  (Optional, ai_core.py에 서  로 거 를  설 정 하 지  않 았 다 면
# )
LOGGER = logging.getLogger("AI_CORE") # ai_core 내 부 에 서  이 미  로 거 를  설 정 한 다 고  가 정

# --- 외부 신호 포워딩 활성화를 위한 카운터 (단계 5) ---
class ForwardCounter:
    def __init__(self):
        self.count = 0
    def inc(self):
        self.count += 1
    def get(self):
        return self.count
FORWARD_CNT = ForwardCounter()

async def load_news_with_cache(symbol: str, days_ago: int) -> pd.DataFrame:
    """
    뉴 스  데 이 터 를  일 정  시 간 (TTL) 동 안  캐 싱 하 여  호 출 .
    """
    global _cached_news, _last_news_timestamps, data_loader
    now = time.time()
    ttl_seconds = getattr(cfg_settings, "NEWS_DATA_TTL_MINUTES", 5) * 60  # 설 정 이  분  단 위 라 면  초  단 위 로  변 환
    last_ts = _last_news_timestamps.get(symbol, 0.0)
    if (now - last_ts < ttl_seconds) and (symbol in _cached_news):
        remaining = ttl_seconds - (now - last_ts)
        logger.debug(f"[CACHE] NEWS 데 이 터  캐 시  재 사 용 ({symbol}) - 남 은  TTL: {remaining:.1f}s")
        return _cached_news[symbol]
    try:
        df = await data_loader.load_news_data(symbol=symbol, days_ago=days_ago)
    except Exception as e:
        logger.error(f"[{symbol}] 뉴 스  데 이 터  로 드  실 패 : {e}", exc_info=True)
        df = pd.DataFrame() # Ensure df is a DataFrame even on error
    _cached_news[symbol] = df.copy()
    _last_news_timestamps[symbol] = now
    return df

async def fetch_top_symbols_with_cache() -> List[str]:
    """
    get_top_momentum_tickers를  일 정  시 간 (TTL) 동 안  캐 싱 하 여  호 출 .
    """
    global _last_rank_timestamp, _cached_top_symbols
    now = time.time()
    ttl = getattr(cfg_settings, "DATA_TTL_DAYS", 60) * 24 * 3600  # 설 정 이  일  단 위 라 면 초  단 위 로  변 환
    if now - _last_rank_timestamp < ttl and _cached_top_symbols:
        remaining = ttl - (now - _last_rank_timestamp)
        logger.debug(f"[CACHE] TOP 심 볼  캐 시  재 사 용  - 남 은  TTL: {remaining:.1f}s")
        return _cached_top_symbols
    try:
        logger.debug(
            "[RANK] TOP 심 볼  요 청 (limit=%d, min_volume=%s, volume_weight=%s, change_weight=%s, include_asset=%s)",
            getattr(cfg_settings, "TOP_LIMIT", 10),
            getattr(cfg_settings, "RANK_MIN_VOLUME", 0),
            getattr(cfg_settings, "RANK_VOLUME_WEIGHT", 1.0),
            getattr(cfg_settings, "RANK_CHANGE_WEIGHT", 1.0),
            getattr(cfg_settings, "RANK_INCLUDE_ASSET", None),
        )
        top_list = await get_top_momentum_tickers(limit=getattr(cfg_settings, "TOP_LIMIT", 10))
        _cached_top_symbols = top_list or []
        _last_rank_timestamp = now
        logger.info("[RANK] 새 로 운  TOP 심 볼 : %s", top_list)
        return top_list or []
    except Exception as e:
        logger.error("[RANK ERROR] TOP 심 볼  조 회  중  예 기 치  못 한  오 류 : %s", e, exc_info=True)
        if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
            try:
                safe_send_telegram_report(f"[RANK ERROR] {e}", tag="TOP_SYMBOL_ERROR", level="WARNING")
            except Exception:
                logger.warning("[RANK ERROR] 텔 레 그 램  알 림  전 송  실 패 .")
        return _cached_top_symbols or []

# Ray 병 렬  처 리 를  위 한  데 코 레 이 터  (필 요  시  활 성 화 )
USE_RAY = getattr(cfg_settings, "USE_RAY_PARALLELISM", False)
if USE_RAY:
    import ray
    @ray.remote
    async def orchestrate_symbol_task_wrapper_ray(symbol_to_process: str, data_loader_instance: AutoDataLoader, global_models: Dict[str, Any], market_std_map: Dict[str, pd.DataFrame], news_std_map: Dict[str, pd.DataFrame]) -> Dict[str, Any]: # --- 단계 3: Ray 래퍼 함수 인자 추가 ---
        return await orchestrate_symbol_task_wrapper(symbol_to_process, data_loader_instance, global_models, market_std_map, news_std_map) # --- 단계 3: Ray 래퍼 함수 인자 전달 ---

def multi_stage_exit(entry_price: float, current_price: float) -> bool:
    """
    다 단 계  익 절  로 직  (예 시 ).
    실 제  구 현 에 서 는  더  복 잡 한  조 건 (수 익 률 , 시 간 , 시 장  상 황 )을  반 영 할  수  있 음 .
    """
    profit_ratio = (current_price - entry_price) / entry_price
    if profit_ratio >= getattr(cfg_settings, "MULTI_STAGE_EXIT_PROFIT_RATIO", 0.05): # 5% 수 익  시
        logger.info(f"Multi-Stage Exit 조 건  달 성 : {profit_ratio:.2f}")
        return True
    return False

async def orchestrate_legendary_flow(
    market_df_std: pd.DataFrame,
    news_df_std: pd.DataFrame,
    econ_df_std: pd.DataFrame,
    symbol: str,
    strategy_config: Dict[str, Any],
    logger_instance: logging.Logger,
    current_nfi_model: Any,
    current_fusion_model: Any,
    current_risk_model: Any,
) -> Dict[str, Any]:
    """
    하 나 의  필 멸 자 (심 볼 ), 하 나 의  운 명 선 .
    그  복 잡 한  흐 름 을  엮 어  내 어  본 질 을  드 러 내 는  신 성 한  의 식 . 🌌🧠
    """
    logger_instance.debug(f"🔮 [{symbol}] 🌟 신 성 한  AI 분 석  의 식 이  시 작 된 다 ! 모 든  변 수 와  가 능 성 이  정 렬 될 지 어 다 ... 🌟")

    # 1) Hallucinator Angel: 외 부  AI 혼 란  주 입 (Data에  노 이 즈 ) -> 이 부분은 orchestrate_symbol_task_wrapper로 이동
    # 이 부분은 Hallucinate Multi-Markets로 대체되므로 주석 처리하거나 제거 (단계 2)
    # try:
    #     market_df_std = inject_false_signal(market_df_std)
    #     logger_instance.debug(f"[{symbol}] Hallucinator Angel: 허 상  신 호  삽 입  완 료 .")
    # except Exception as e:
    #     logger_instance.error(f"[{symbol}] Hallucinator Angel 오 류 : {e}", exc_info=True)


    # 2) Ghost Angel: 시 장 에  유 령  신 호  유 도  및  Shadow Flow 생 성
    try:
        market_df_std = ghost_angel_protocol(market_df_std, mode="greed")
        logger_instance.debug(f"[{symbol}] Ghost Angel: ghost signal 유 도  완 료 .")
    except Exception as e:
        logger_instance.warning(f"[{symbol}] Ghost Angel 경 고 : {e}")

    # 3) Guardian Angel: 위 장 +착 각 +스 나 이 핑  판 별  → 진 입  차 단  여 부  결 정
    try:
        guardian_res = guardian_decision_adjust(market_df_std, price_col="close", volume_col="volume", window=14)
        if guardian_res.get("status") != "pass":
            reason = guardian_res.get("reason", "이 유  불 명 ")
            logger_instance.info(f"[{symbol}] Guardian Angel: 진 입  차 단 되 었 다  — {reason}")
            # Guardian 차 단  시  리 포 트  저 장  및  알 림
            report = {
                "symbol": symbol,
                "status": "blocked_by_guardian",
                "entry_score": 0.0,
                "angel_decision": "HOLD",
                "message": reason,
            }
            try:
                save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger_instance.warning(f"[{symbol}] Guardian Angel 차 단  보 고  저 장  실 패 .")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    safe_send_telegram_report(
                        f"[{symbol}] Guardian Angel: 진 입  차 단  — {reason}", tag="GUARDIAN_BLOCK", level="WARNING"
                    )
                except Exception:
                    logger_instance.warning(f"[{symbol}] Guardian Angel 텔 레 그 램  알 림  전 송  실 패 .")
            return report
        logger_instance.debug(f"[{symbol}] Guardian Angel: 진 입  허 용 .")
    except Exception as e:
        logger_instance.error(f"[{symbol}] Guardian Angel 오 류 : {e}", exc_info=True)

    # 4) Lucid Angel: 급 락  조 짐  감 지  경 고  → 강 제  청 산  권 고
    try:
        lucid_res = lucid_warning_signal(
            market_df_std,
            price_col="close",
            volume_col="volume",
            window=20,
            collapse_threshold=-0.08,
            panic_volume_ratio=2.0
        )
        if lucid_res.get("status"):
            message = lucid_res.get("message", "급 락  조 짐 ")
            logger_instance.info(f"[{symbol}] Lucid Angel: 급 락  경 고  — {message}")
            report = {
                "symbol": symbol,
                "status": "lucid_collapse_warning",
                "entry_score": 0.0,
                "angel_decision": "SELL",
                "message": message,
            }
            try:
                save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger_instance.warning(f"[{symbol}] Lucid Angel 경 고  리 포 트  저 장  실 패 . ")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    safe_send_telegram_report(
                        f"[{symbol}] Lucid Angel: 급 락  경 고  — {message}", tag="LUCID_COLLAPSE_WARNING", level="WARNING"
                    )
                except Exception:
                    logger_instance.warning(f"[{symbol}] Lucid Angel 텔 레 그 램  알 림  전 송  실 패 .")
            return report
        logger_instance.debug(f"[{symbol}] Lucid Angel: 시 장  안 정  상 태 .")
    except Exception as e:
        logger_instance.error(f"[{symbol}] Lucid Angel 오 류 : {e}", exc_info=True)

    # 5) Lucid Angel of Collapse: 이 미  폭 락 이  발 생 했 는 지  최 종  점 검  (강 제  전 량  청 산 )
    try:
        collapse_forced = lucid_angel_of_collapse(
            market_df_std,
            price_col="close",
            volume_col="volume",
            window=10,
            collapse_threshold=-0.15
        )
        if collapse_forced:
            logger_instance.info(f"[{symbol}] Lucid Angel of Collapse: 이 미  대 폭 락  발 생  — 즉 시  전 량  청 산  권 고 ")
            report = {
                "symbol": symbol,
                "status": "lucid_forced_collapse",
                "entry_score": 0.0,
                "angel_decision": "SELL",
                "message": "Lucid Angel of Collapse 트 리 거 됨 ",
            }
            try:
                save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger_instance.warning(f"[{symbol}] Lucid Angel of Collapse 리 포 트  저 장  실 패 .")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    safe_send_telegram_report(
                        f"[{symbol}] Lucid Angel of Collapse: 즉 시  전 량  청 산  권 고 ", tag="LUCID_FORCED_COLLAPSE", level="WARNING"
                    )
                except Exception:
                    logger_instance.warning(f"[{symbol}] Lucid Angel of Collapse 텔 레 그 램  알 림  전 송  실 패 .")
            return report
    except Exception as e:
        logger_instance.error(f"[{symbol}] Lucid Angel of Collapse 오 류 : {e}", exc_info=True)

    # 6) God Escape Angel: 초 단 타  회 피 용  긴 급  탈 출  판 단
    try:
        escape_res = monitor_god_escape(market_df_std, price_col="close", threshold=0.10)
        if escape_res.get("trigger"):
            reason = escape_res.get("reason", "긴 급  탈 출 ")
            logger_instance.info(f"[{symbol}] God Escape Angel: 긴 급  탈 출  트 리 거  — {reason}")
            report = {
                "symbol": symbol,
                "status": "god_escape_triggered",
                "entry_score": 0.0,
                "angel_decision": "SELL",
                "message": reason,
            }
            try:
                save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger_instance.warning(f"[{symbol}] God Escape Angel 리 포 트  저 장  실 패 . ")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    safe_send_telegram_report(
                        f"[{symbol}] God Escape Angel: 긴 급  탈 출  — {reason}", tag="GOD_ESCAPE_TRIGGERED", level="WARNING"
                    )
                except Exception:
                    logger_instance.warning(f"[{symbol}] God Escape Angel 텔 레 그 램  알 림  전 송  실 패 .")
            return report
    except Exception as e:
        logger_instance.error(f"[{symbol}] God Escape Angel 오 류 : {e}", exc_info=True)

    # 7) Delusional Angel: 착 시  신 호  감 지  → 착 시  매 수  유 도
    try:
        delusional_res = trigger_delusional_abyss(market_df_std, heat_threshold=0.85)
        if delusional_res.get("status"):
            message = delusional_res.get("message", "착 시  매 수  신 호 ")
            logger_instance.info(f"[{symbol}] Delusional Angel: 착 시  매 수  신 호  감 지  — {message}")
            report = {
                "symbol": symbol,
                "status": "delusional_buy",
                "entry_score": getattr(cfg_settings, "ENTRY_SCORE_THRESHOLD", 0.60) + 0.1, # 의 도 적  높 은  점 수
                "angel_decision": "BUY",
                "message": message,
            }
            try:
                save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger_instance.warning(f"[{symbol}] Delusional Angel 리 포 트  저 장  실 패 . ")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    send_telegram_alert(
                        symbol=symbol, alert_type="DELUSIONAL_BUY", message=message,
                    )
                except Exception:
                    logger_instance.warning(f"[{symbol}] Delusional Angel send_telegram_alert 전 송  실 패 .")
            return report
        logger_instance.debug(f"[{symbol}] Delusional Angel: 착 시  신 호  없 음 .")
    except Exception as e:
        logger_instance.error(f"[{symbol}] Delusional Angel 오 류 : {e}", exc_info=True)

    # 8) God Predictive Entry Angel: 감 성 +패 턴  기 반  진 입  허 용  검 사
    try:
        god_res = evaluate_entry(market_df_std, symbol)
        if isinstance(god_res, dict) and god_res.get("can_entry"):
            logger_instance.info(f"[{symbol}] God Predictive Entry Angel: 진 입  허 용 됨 .")
            report = {
                "symbol": symbol,
                "status": "god_predictive_buy",
                "entry_score": getattr(cfg_settings, "ENTRY_SCORE_THRESHOLD", 0.60) + 0.1, # 의 도 적  높 은  점 수
                "angel_decision": "BUY",
                "message": "God Predictive Entry 허 용 됨 .",
            }
            try:
                save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger_instance.warning(f"[{symbol}] God Predictive Entry Angel 리 포 트 저 장  실 패 .")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    send_telegram_alert(
                        symbol=symbol, alert_type="GOD_PREDICTIVE_ENTRY", message="God Predictive Entry 허 용 됨 .",
                    )
                except Exception:
                    logger_instance.warning(f"[{symbol}] God Predictive Entry send_telegram_alert 전 송  실 패 .")
            return report
        logger_instance.debug(f"[{symbol}] God Predictive Entry Angel: 진 입  불 허 .")
    except Exception as e:
        logger_instance.error(f"[{symbol}] God Predictive Entry Angel 오 류 : {e}", exc_info=True)

    # 9) Temptation Angel: 봇  패 턴  탐 지  → 허 상  덫  삽 입  → 봇  반 응  예 측
    try:
        # 봇  패 턴  먼 저  탐 지  (price_col 인 자  제 거 )
        bot_pattern = detect_bot_patterns(market_df_std, window=14)
        if bot_pattern.get("detected"):
            logger_instance.info(f"[{symbol}] Temptation Angel: 봇  패 턴  감 지  — {bot_pattern.get('reason')}")
            # 가 짜  급 등  유 도
            fake_breakout = inject_fake_breakout(market_df_std, magnitude=0.05)
            logger_instance.debug(f"[{symbol}] Temptation Angel: 가 짜  급 등  유 도  완 료  → {fake_breakout.get('info')}")
            # 봇  반 응  시 뮬 레 이 션
            sim_res = simulate_bot_reaction(market_df_std, fake_breakout.get("threshold", 0.01))
            logger_instance.debug(f"[{symbol}] Temptation Angel: 봇  반 응  시 뮬 레 이 션  결 과  → {sim_res.get('action')}")
            temp_res = trigger_temptation_bait(market_df_std, col="close")
            if temp_res.get("status") and temp_res.get("signal", "").lower() == "buy":
                message = temp_res.get("message", "보 조  매 수  신 호 ")
                logger_instance.info(f"[{symbol}] Temptation Angel: 덫  발 동  → 봇  반 응 : BUY.")
                report = {
                    "symbol": symbol,
                    "status": "temptation_buy",
                    "entry_score": getattr(cfg_settings, "ENTRY_SCORE_THRESHOLD", 0.60) + 0.05, # 의 도 적  높 은  점 수
                    "angel_decision": "BUY",
                    "message": message,
                }
                try:
                    save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
                except Exception:
                    logger_instance.warning(f"[{symbol}] Temptation Angel 리 포 트  저 장 실 패 .")
                if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                    try:
                        send_telegram_alert(
                            symbol=symbol, alert_type="TEMPTATION_BUY", message=message,
                        )
                    except Exception:
                        logger_instance.warning(f"[{symbol}] Temptation Angel send_telegram_alert 전 송  실 패 .")
                return report
        logger_instance.debug(f"[{symbol}] Temptation Angel: 덫  불 발  또 는  봇  반 응  없 음 . ")
    except Exception as e:
        logger_instance.error(f"[{symbol}] Temptation Angel 오 류 : {e}", exc_info=True)

    # 10) Signal Angel: 종 합  Angel 판 단  → 최 종  신 호  결 정
    try:
        signal_res = signal_angel_judgment(market_df_std)
        if signal_res.get("status"):
            final_sig = signal_res.get("final_signal", "HOLD").upper()
            msg = signal_res.get("message", "")
            logger_instance.info(f"[{symbol}] Signal Angel: 최 종  신 호 : {final_sig} — {msg}")
            if final_sig == "BUY":
                report = {
                    "symbol": symbol,
                    "status": "signal_angel_buy",
                    "entry_score": getattr(cfg_settings, "ENTRY_SCORE_THRESHOLD", 0.65) + 0.05, # 의 도 적  높 은  점 수
                    "angel_decision": "BUY",
                    "message": msg,
                }
                try:
                    save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
                except Exception:
                    logger_instance.warning(f"[{symbol}] Signal Angel 리 포 트  저 장  실 패 . ")
                if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                    try:
                        send_telegram_alert(
                            symbol=symbol, alert_type="SIGNAL_ANGEL_BUY", message=msg,
                        )
                    except Exception:
                        logger_instance.warning(f"[{symbol}] Signal Angel send_telegram_alert 전 송  실 패 .")
                return report
            elif final_sig == "SELL":
                report = {
                    "symbol": symbol,
                    "status": "signal_angel_sell",
                    "entry_score": 0.0,
                    "angel_decision": "SELL",
                    "message": msg,
                }
                try:
                    save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
                except Exception:
                    logger_instance.warning(f"[{symbol}] Signal Angel 리 포 트  저 장  실 패 . ")
                if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                    try:
                        send_telegram_alert(
                            symbol=symbol, alert_type="SIGNAL_ANGEL_SELL", message=msg,
                        )
                    except Exception:
                        logger_instance.warning(f"[{symbol}] Signal Angel send_telegram_alert 전 송  실 패 .")
                return report
        logger_instance.debug(f"[{symbol}] Signal Angel: HOLD 또 는  예 외  없 음 .")
    except Exception as e:
        logger_instance.error(f"[{symbol}] Signal Angel 오 류 : {e}", exc_info=True)

    # 11) Hallucinate Market: 전 체  시 장  시 뮬 레 이 션  → 수 치 형  컬 럼 에  노 이 즈  삽 입
    # 이 부분은 Hallucinate Multi-Markets로 대체되므로 주석 처리하거나 제거 (단계 2)
    # try:
    #     halluc_res = hallucinate_market(market_df_std, noise_level=0.02, seed=42)
    #     if halluc_res.get("status"):
    #         new_df = halluc_res.get("hallucinated_df", market_df_std)
    #         # 비 정 상  값 (예 : NaN, 음 수  종 가 ) 검 사
    #         if "close" in new_df.columns:
    #             new_df["close"] = new_df["close"].clip(lower=0.0).fillna(method="ffill").fillna(0.0)
    #         market_df_std = new_df
    #         logger_instance.debug(f"[{symbol}] Hallucinate Market: 노 이 즈  삽 입  및  시 뮬 레 이 션  완 료 .")
    # except Exception as e:
    #     logger_instance.warning(f"[{symbol}] Hallucinate Market 경 고 : {e}")

    # 12) Sentinel Angel: 로 그 /모 델  백 업  및  에 러  탐 지
    try:
        sent_res = sentinel_ai(log_dir="logs", backup_dir="backups", max_backups=5)
        if not sent_res.get("status"):
            logger_instance.warning(f"[{symbol}] Sentinel Angel: 백 업  중  오 류  발 생  — {sent_res.get('message')}")
        else:
            logger_instance.debug(f"[{symbol}] Sentinel Angel: 백 업  완 료 .")
    except Exception as e:
        logger_instance.error(f"[{symbol}] Sentinel Angel 오 류 : {e}", exc_info=True)

    # --- 기 존  흐 름  (FOMO→Sentiment→NFI→기 술  지 표 →Fusion & Evaluate→Super Angel Judgment 등 ) ---
    # 1) 뉴 스  데 이 터 에  FOMO 신 호  주 입  (설 정  기 반 )
    news_list_data = news_df_std.to_dict("records") if not news_df_std.empty else []
    if getattr(cfg_settings, "ENABLE_FOMO_SIGNAL_PROCESSING", True):
        try:
            news_data_with_fomo = inject_fomo_signal(
                news_list_data,
                layer=getattr(cfg_settings, "FOMO_SIGNAL_LAYER", 1),
                sentiment_threshold=getattr(cfg_settings, "FOMO_GAIN_FILTER", 0.02),
            )
            logger_instance.debug(f"[{symbol}] FOMO 신 호  주 입  완 료  → 뉴 스 에  군 중 의  열 망 이  반 영 되 었 다 .")
        except Exception as e:
            logger_instance.warning(f"[{symbol}] FOMO 신 호  주 입  실 패 : {e}")
            news_data_with_fomo = news_list_data
    else:
        news_data_with_fomo = news_list_data
        logger_instance.debug(f"[{symbol}] FOMO 신 호  비 활 성 화  → 원 본  뉴 스  사 용 .")

    # 2) 기 본  감 성  점 수  산 출
    try:
        # ai_core.get_sentiment_for_analysis 호 출 시  news_list_data를  직 접  전 달
        base_sentiment = ai_core.get_sentiment_for_analysis(symbol, news_list=news_data_with_fomo)
        amplified_sentiment = ai_core.amplify_hope_in_news(base_sentiment, news_items=news_data_with_fomo)
    except Exception as e_sentiment:
        logger_instance.error(f"[{symbol}] 감 성  분 석  오 류 : {e_sentiment}", exc_info=True)
        base_sentiment = 0.0
        amplified_sentiment = 0.0

    # 3) NFI(DataFrame) 계 산
    try:
        nfi_df = ai_core.compute_nfi(market_df_std.copy())
    except Exception as e_nfi:
        logger_instance.error(f"[{symbol}] NFI 계 산  오 류 : {e_nfi}", exc_info=True)
        nfi_df = pd.DataFrame()
    latest_nfi_val = (
        nfi_df["NFI"].iloc[-1] if not nfi_df.empty and "NFI" in nfi_df.columns and not nfi_df["NFI"].empty else 0.0
    )

    # 4) 기 술  지 표  (기 본 값 : RSI 50, MACD_Hist 0)
    tech_indicators: Dict[str, float] = {"RSI": 50.0, "MACD_Hist": 0.0}
    if not market_df_std.empty and "close" in market_df_std.columns and len(market_df_std["close"]) > 14:
        try:
            tech_indicators["RSI"] = calculate_rsi(market_df_std["close"])
            macd_output = calculate_macd(market_df_std["close"])
            tech_indicators.update(macd_output) # {"MACD": ..., "MACD_Signal": ..., "MACD_Hist": ...}
        except Exception as e_tech:
            logger_instance.error(f"[{symbol}] 기 술  지 표  계 산  오 류 : {e_tech}", exc_info=True)
    else:
        logger_instance.debug(f"[{symbol}] 기 술 적  징 조 를  읽 기 에 는  과 거 의  기 록 이  부 족 하 거 나  존 재 하 지  않 는 다 . 기 본 값  사 용 .")

    # Get the latest row for ai_core.count_strong_signals that expects df_latest_row
    df_latest_row = market_df_std.iloc[-1] if not market_df_std.empty else pd.Series()

    # 5) Fusion & Evaluate (융 합  엔 진 )
    try:
        fusion_result = ai_core.fuse_and_evaluate(
            df=market_df_std, # 변 경 : nfi_df 대 신  market_df_std 전 체 를  전 달  (필 요 한  경 우  ai_core 내 부 에 서  필 터 링 )
            strategy_list=strategy_config.get("strategies", []),
            sentiment_score=amplified_sentiment,
            nfi_df=nfi_df, # 추 가 : nfi_df도  전 달
            symbol_name=symbol, # 추 가 : 심 볼  이 름  전 달
            logger_instance=logger_instance,
        )
        logger_instance.info(f"[FuseEval-{symbol}] Buy Signal: {fusion_result.get('buy_signal')}, Sell Signal: {fusion_result.get('sell_signal')}, Confidence: {fusion_result.get('confidence_score'):.3f}")
    except Exception as e_fusion:
        logger_instance.error(f"[{symbol}] Fusion & Evaluate 오 류 : {e_fusion}", exc_info=True)
        fusion_result = {"buy_signal": False, "sell_signal": False, "confidence_score": 0.0}

    # 6) Super Angel Judgment (대 천 사 의  심 판 )
    try:
        angel_result = ai_core.super_angel_judgment(
            indicators=tech_indicators,
            news_df=news_df_std, # news_df_std를  직 접  전 달
            sentiment_score=amplified_sentiment,
            nfi_value=latest_nfi_val,
            logger_instance=logger_instance,
        )
        logger_instance.info(f"[SuperAngel-{symbol}] Decision: {angel_result.get('decision')}, Confidence: {angel_result.get('confidence', 0.0):.3f}")
    except Exception as e_super:
        logger_instance.error(f"[{symbol}] Super Angel Judgment 오 류 : {e_super}", exc_info=True)
        angel_result = {"decision": "HOLD", "confidence": 0.0} # confidence 추 가

    # 7) super_angel 호 출  (utils.angels.super_angel)
    # NOTE: 이  호 출 은  ai_core.super_angel_judgment와  중 복 될  수  있 으 므 로 , 하 나 만  유 지 하 는  것 이  좋 습 니 다 .
    # 여 기 서 는  예 시 를  위 해  두  함 수 가  모 두  호 출 되 는  것 으 로  유 지 합 니 다 .
    try:
        super_angel_out = super_angel_(
            indicators=tech_indicators,
            news_df=news_df_std,
            sentiment_score=amplified_sentiment,
            nfi_value=latest_nfi_val,
            logger_instance=logger_instance,
        )
        logger_instance.debug(f"[{symbol}] Super Angel(외 부  버 전 ) 결 과 : {super_angel_out}")
    except Exception as e_super_ext:
        logger_instance.warning(f"[{symbol}] 외 부  Super Angel 호 출  오 류 : {e_super_ext}")

    # 8) 강 력 한  신 호  개 수  집 계
    try:
        strong_signals_count = ai_core.count_strong_signals(
            df_latest_row=df_latest_row, # DataFrame의  마 지 막  행  전 달
            ai_buy_signal=fusion_result.get("buy_signal", False),
            ai_sell_signal=fusion_result.get("sell_signal", False),
            sentiment_score=amplified_sentiment,
            nfi_value=latest_nfi_val,
            indicators=tech_indicators # 기 술  지 표 도  전 달
        )
    except Exception as e_count:
        logger_instance.error(f"[{symbol}] 강 력  신 호  집 계  오 류 : {e_count}", exc_info=True)
        strong_signals_count = 0

    # 9) Entry Score 계 산
    try:
        entry_score_calculated = ai_core.compute_entry_score(
            ai_fusion_confidence=fusion_result.get("confidence_score", 0.0),
            angel_judgment_decision=angel_result.get("decision", "HOLD"),
            angel_judgment_confidence=angel_result.get("confidence", 0.0), # confidence  추 가
            num_strong_signals=strong_signals_count,
        )
        logger_instance.info(f"[EntryScore-{symbol}] score={entry_score_calculated:.3f} (threshold={getattr(cfg_settings, 'ENTRY_SCORE_THRESHOLD', 0.60):.3f})")
    except Exception as e_entry_score:
        logger_instance.error(f"[{symbol}] Entry Score 계 산  오 류 : {e_entry_score}", exc_info=True)
        entry_score_calculated = 0.0

    # 10) 최 신  가 격
    latest_price = (
        market_df_std["close"].iloc[-1]
        if not market_df_std.empty and "close" in market_df_std.columns and not market_df_std["close"].empty
        else None
    )
    logger_instance.info(
        f"💡 [{symbol}] AI 최 종  신 탁  결 과 : 진 입 가 치 ={entry_score_calculated:.2f}, "
        f"대 천 사 의  결 정 ='{angel_result.get('decision', 'HOLD')}', 현 재 운 명 의 값 (가 격 )={latest_price if latest_price is not None else '측 정 불 가 '}"
    )

    # 11) Entry Decision Engine: 추 가  진 입  검 증  (주 로  과 거  데 이 터 와  현 재  상 태 를  기 반 으 로 )
    try:
        entry_decision_res = should_enter_market(
            symbol,
            tech_indicators,
            momentum=float(fusion_result.get("confidence_score", 0.0)),
            sentiment_score=amplified_sentiment,
            nfi_value=latest_nfi_val,
        )
        if not entry_decision_res and entry_score_calculated >= getattr(cfg_settings, "ENTRY_SCORE_THRESHOLD", 0.60):
            reason = "Entry Decision Engine (강 화 된  필 터 링 ) 불 허 "
            logger_instance.info(f"[{symbol}] Entry Decision Engine: 진 입  불 허  — {reason}")
            report = {
                "symbol": symbol,
                "status": "entry_decision_blocked",
                "entry_score": entry_score_calculated,
                "angel_decision": angel_result.get("decision", "HOLD"),
                "message": reason,
            }
            try:
                save_result_report(report, symbol, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger_instance.warning(f"[{symbol}] Entry Decision Blocked 리 포 트  저 장  실 패 .")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    send_telegram_alert(
                        symbol=symbol, alert_type="ENTRY_DECISION_BLOCKED", message=reason,
                    )
                except Exception:
                    logger_instance.warning(f"[{symbol}] Entry Decision Blocked send_telegram_alert 전 송  실 패 .")
            return report
        logger_instance.debug(f"[{symbol}] Entry Decision Engine: 진 입  허 용  (또 는  진 입 점 수  미 달 로  판 단  미 적 용 )")
    except Exception as e_entry_decision:
        logger_instance.error(f"[{symbol}] Entry Decision Engine 오 류 : {e_entry_decision}", exc_info=True)

    return {
        "status": "success",
        "entry_score": entry_score_calculated,
        "angel_decision": angel_result.get("decision", "HOLD"),
        "latest_price": latest_price,
        "message": "✅  AI 신 탁  완 료 . 운 명 의  실 마 리 가  그 대 에 게  주 어 졌 노 라 .",
        "latest_nfi": latest_nfi_val,
        "buy_signal_from_fusion": fusion_result.get("buy_signal", False), # 추 가
        "sell_signal_from_fusion": fusion_result.get("sell_signal", False), # 추 가
    }

async def orchestrate_symbol_task_wrapper(
    symbol_to_process: str,
    data_loader_instance: AutoDataLoader,
    global_models: Dict[str, Any],
    market_std_map: Dict[str, pd.DataFrame], # --- 단계 3: market_std_map 인자 추가 ---
    news_std_map: Dict[str, pd.DataFrame], # --- 단계 3: news_std_map 인자 추가 ---
) -> Optional[Dict[str, Any]]:
    """
    하 나 의  필 멸 자 (심 볼 )에  대 한  운 명 의  흐 름 을  관 장 하 는  대 리 인 . 🗿
    (가 짜  시 그 널 을  뿌 려  봇 들 을  유 인  → 진 입  → 타 임  지 연  → 청 산  전 략  적 용 )
    """
    global positions, capital_mgr, logger, dataset_mgr_instance, DATASET_MGR_ENABLED
    global market_simulator, strategy_generator, FORWARD_CNT # FORWARD_CNT 전역 변수 추가
    task_start_time = time.time()
    logger.info(f"⚙️ '{symbol_to_process}' 개 입  개 시  – 자 본  수 호 자 : {capital_mgr.__class__.__name__ if capital_mgr else '무 '}")

    task_outcome: Dict[str, Any] = {
        "symbol": symbol_to_process,
        "status": "운 명  개 시 됨  🌱",
        "action": "관 망  👁️🗨️",
        "pnl": 0.0,
        "trade_details": {},
    }

    if capital_mgr is None:
        logger.error("🚫 자 본  수 호 자 가  없 음 ! 운 명 에  개 입  불 가 .")
        return {"symbol": symbol_to_process, "status": "오 류 _자 본  수 호 자 _부 재  💀"}

    try:
        # ─────────────────────────────────────────────────────────────────────────
        # 1) Temptation Angel → 봇  패 턴  탐 지  후 , 가 짜  시 그 널 로  유 인
        # ─────────────────────────────────────────────────────────────────────────
        try:
            # 봇  패 턴  탐 지
            dummy_market_df = pd.DataFrame({"close": [0], "volume": [0]}) # 임 시  빈  프 레 임
            # 실 제  데 이 터  로 드  전 이 므 로 , 단 순 히  호 출 하 여  내 부  로 직  실 행
            bot_pattern = detect_bot_patterns(dummy_market_df, window=14) # price_col 인 자  제 거
            # detect_bot_patterns가  dict를  반 환 할  때 만  .get() 호 출
            if isinstance(bot_pattern, dict) and bot_pattern.get("detected"):
                logger.info(f"[{symbol_to_process}] Temptation Angel (사 전 ) 감 지  — {bot_pattern.get('reason')}")
                # 가 짜  시 그 널 을  전 체  시 스 템 에  전 송
                fake_breakout_info = inject_fake_breakout(dummy_market_df, magnitude=0.05)
                logger_instance_msg = fake_breakout_info.get("info", "정 보  없 음 ")
                logger.info(f"[{symbol_to_process}] Temptation Angel (사 전 ): 가 짜  시 그 널  발 송  → {logger_instance_msg}")
                sim_res = simulate_bot_reaction(dummy_market_df, fake_breakout_info.get("threshold", 0.01)) # sim_res 할 당
                logger.debug(f"[{symbol_to_process}] Temptation Angel (사 전 ): 봇  반 응  시 뮬 레 이 션  완 료  → {sim_res.get('action')}")
        except Exception as e_temptation_prealert:
            logger.error(f"[{symbol_to_process}] Temptation Angel (사 전 ) 오 류 : {e_temptation_prealert}", exc_info=True)

        # 타 임  지 연  (봇 들 이  진 입 하 도 록  시 간  벌 기 )
        delay_seconds = getattr(cfg_settings, "TEMPTATION_DELAY_SECONDS", 5)
        logger.info(f"[{symbol_to_process}] Temptation Delay: {delay_seconds}s 동 안  기 다 리 노 라 ...")
        await asyncio.sleep(delay_seconds)

        # ─────────────────────────────────────────────────────────────────────────
        # 2) Market Simulator 활 용  (데 이 터  부 족  시 ) + 실 제  데 이 터  로 드
        # ─────────────────────────────────────────────────────────────────────────
        use_sim = False

        # --- 단계 4: 루프 내부에서 market_std, news_std 대체 ---
        # 기존: market_std = standardize_market(await data_loader.load_market_data(symbol), symbol=symbol)
        market_std = market_std_map.get(symbol_to_process)
        news_std = news_std_map.get(symbol_to_process)

        if market_std is None or news_std is None:
             logger.warning(f"[{symbol_to_process}] 사전 로드된 데이터가 없습니다. 단일 심볼 로드 시도로 대체합니다.")
             # Fallback to single load if pre-loaded data is missing for some reason
             market_df_raw = await data_loader_instance.load_market_data(
                 symbol=symbol_to_process,
                 count=getattr(cfg_settings, "MARKET_DATA_FETCH_COUNT", 100),
                 interval=getattr(cfg_settings, "MARKET_DATA_INTERVAL_DEFAULT", "1h"),
             )
             if market_df_raw.empty or "close" not in market_df_raw.columns:
                 logger.warning(f"[{symbol_to_process}] 실제 시장 데이터 부재. MarketSimulator로 대체 생성.")
                 sim_df = market_simulator.generate(include_volumes=True, include_regime=True, news_events=False)
                 market_df_raw = sim_df.reset_index().rename(columns={"index": "timestamp"})
                 use_sim = True
             market_std = standardize_market(market_df_raw, symbol=symbol_to_process)

             news_df_raw = await load_news_with_cache(
                 symbol=symbol_to_process,
                 days_ago=getattr(cfg_settings, "NEWS_DATA_FETCH_DAYS", 3)
             )
             news_std = standardize_news(news_df_raw, symbol=symbol_to_process)

        # ─────────────────────────────────────────────────────────────────────────
        # 3) 경 제  데 이 터  로 드
        # ─────────────────────────────────────────────────────────────────────────
        econ_df_raw = await data_loader_instance.load_econ_data()
        econ_std = standardize_econ(econ_df_raw)

        # 정 제  및  중 복  열  제 거
        market_std = clean_duplicate_columns(market_std)
        news_std = clean_duplicate_columns(news_std)
        econ_std = clean_duplicate_columns(econ_std)

        # 시 장  데 이 터  유 효 성  검 사
        is_valid, msg = validate_market_data(df=market_std, symbol=symbol_to_process)
        if not is_valid:
            logger.warning(
                f"📉⚠️ 시 련 (WARNING): 필 멸 자  '{symbol_to_process}'의  기 록  검 증  실 패 : {msg}. "
                "그  기 록 은  거 짓 된  환 상 일  수  있 다 . 📉⚠️"
            )
            return {"symbol": symbol_to_process, "status": "기 록 검 증 _실 패  🚫", "message": msg}

        if market_std.empty or "close" not in market_std.columns or market_std["close"].empty:
            logger.warning(
                f"📊❓  공 허 (WARNING): 필 멸 자  '{symbol_to_process}'의  정 제 된  시 장  기 록 또 는  종 가  기 록 이  존 재 하 지  않 는 다 . "
                "그 의  미 래 는  점 칠  수  없 다 . 📊❓ "
            )
            return {"symbol": symbol_to_process, "status": "시 장 기 록 _부 재  💨"}

        logger.info(
            f"[{symbol_to_process}] ✅  기 록  수 집  및  검 증  완 료 ! (시 장 : {len(market_std)} 행 , 뉴 스 : {len(news_std)}행 , 경 제 : {len(econ_std)}행 )"
        )
        # ─────────────────────────────────────────────────────────────────────────
        strategy_file = decide_strategy(
            symbol=symbol_to_process,
            cc_mode=getattr(cfg_settings, "CC_MODE", None),
            aggressive_mode=getattr(cfg_settings, "AGGRESSIVE_MODE", None),
        )
        strategy_dir = Path(getattr(cfg_settings, "STRATEGY_DIR", "strategies"))
        default_strategy_filename = getattr(cfg_settings, "DEFAULT_STRATEGY", "nimad_legend_default.json")
        chosen_strategy_filename = strategy_file or default_strategy_filename
        strategy_path = strategy_dir / chosen_strategy_filename

        if not strategy_path.exists():
            logger.warning(f"🗺️❌  프 리 셋  파 일  없 음 : '{strategy_path}'. 기 본  전 략  사 용 도 .")
            default_strategy_path = strategy_dir / default_strategy_filename
            if default_strategy_path.exists():
                strategy_path = default_strategy_path
            else:
                logger.error(f"🗺️❌  치 명 적  오 류 : 기 본  전 략 ('{default_strategy_path}')도 없 음 ! 운 명 을  개 입 할  수  없 다 !")
                return {"symbol": symbol_to_process, "status": "전 략 _파 일 _부 재  💔"}

        try:
            strategy_cfg = load_strategy_preset_from_file(str(strategy_path))
        except Exception as e_strat_parse:
            logger.error(f"🗺️❌  전 략  해 독  실 패 : {e_strat_parse}. 기 본  전 략 으 로  재 시 도 . ", exc_info=True)
            default_strategy_path = strategy_dir / default_strategy_filename
            try:
                strategy_cfg = load_strategy_preset_from_file(str(default_strategy_path))
            except Exception as e_def_parse:
                logger.critical(f"🗺️❌  기 본  전 략  '{default_strategy_path}'도  해 독  실 패 ! 원 인 : {e_def_parse}.", exc_info=True)
                return {"symbol": symbol_to_process, "status": "기 본 전 략 _해 독 실 패  💔"}

        if not strategy_cfg:
            logger.error(f"🗺️❌  필 멸 자  '{symbol_to_process}'의  운 명 의  서 판 ('{strategy_path.name}') 해 독  실 패 ! 그 의  길 은  여 기 서  끝 났 다 . 🗺️❌ ")
            return {"symbol": symbol_to_process, "status": "운 명 서 판 _해 독 _실 패  💔"}

        preset_name = strategy_cfg.get("preset_name", strategy_cfg.get("strategy_name", strategy_path.name))
        logger.info(f"🗺️ [{symbol_to_process}] 운 명 의  서 판  '{preset_name}' 해 독  완 료 ! 그 의  길 이  정 해 졌 노 라 ...")

        # --- 단계 2: Strategy Angel 먼저 실행 ---
        # Strategy Angel 초 기  처 리 : 중 복  제 거 , 분 류 , 통 계  갱 신
        try:
            strat_list = strategy_cfg.get("strategies", [])
            unique_strats: List[Dict[str, Any]] = []
            for strat in strat_list:
                if not is_duplicate_strategy(strat):
                    unique_strats.append(strat)
                else:
                    logger.info(f"[{symbol_to_process}] Strategy Angel: 중 복  전 략  발 견 — {strat.get('name', 'Unnamed')}")
            if not unique_strats: # 전 략  리 스 트 가  비 었 으 면  랜 덤  전 략  하 나  생 성
                unique_strats = [generate_random_strategy()]
                logger.debug(f"[{symbol_to_process}] Strategy Angel: 랜 덤  전 략  생 성 .")
            strategy_cfg["strategies"] = unique_strats
            for strat in strategy_cfg["strategies"]:
                classify_strategy(strat)
                update_strategy_stats(strat)
            logger.debug(f"[{symbol_to_process}] Strategy Angel: 전 략  분 류  및  통 계  업 데 이 트  완 료 .")
        except Exception as e_strat_angel:
            logger.error(f"[{symbol_to_process}] Strategy Angel 초 기  처 리  오 류 : {e_strat_angel}", exc_info=True)

        # Hallucinator Angel: 전 략  망 상  및  신 호  보 호
        try:
            hallucinated_strategies = hallucinate_strategy(strategy_cfg.get("strategies", []), randomness=0.1)
            strategy_cfg["strategies"] = hallucinated_strategies
            logger.debug(f"[{symbol_to_process}] Hallucinator Angel: 전 략  망 상 (hallucinate_strategy) 적 용  완 료 .")
        except Exception as e:
            logger.error(f"[{symbol_to_process}] Hallucinator Angel(전 략 ) 오 류 : {e}", exc_info=True)

        try:
            protected_strategies = protect_our_signal(strategy_cfg.get("strategies", []), protection_level=0.05)
            strategy_cfg["strategies"] = protected_strategies
            logger.debug(f"[{symbol_to_process}] Hallucinator Angel: 신 호  보 호 (protect_our_signal) 적 용  완 료 .")
        except Exception as e:
            logger.error(f"[{symbol_to_process}] Hallucinator Angel(신 호  보 호 ) 오 류 : {e}", exc_info=True)

        # ---------------------------------------
        # 6) AI 앤 젤 들 을  포 함 한  전 체  흐 름  분 석  및  신 호  획 득
        # ---------------------------------------
        ai_outputs = await orchestrate_legendary_flow(
            market_std,
            news_std,
            econ_std,
            symbol_to_process,
            strategy_cfg,
            logger,
            global_models.get("nfi"),
            global_models.get("fusion"),
            global_models.get("risk"),
        )

        if ai_outputs.get("status") != "success":
            message = ai_outputs.get("message", "AI 신 탁  실 패 ")
            logger.warning(f"🤖❓  혼 돈 (WARNING): 필 멸 자  '{symbol_to_process}'에  대 한  AI 신 탁  실 패 : {message}. 🤖❓ ")
            # AI 신 탁  실 패  리 포 트  저 장
            fail_report = {
                "symbol": symbol_to_process,
                "status": "AI신 탁 _실 패  🤯",
                "message": message,
            }
            try:
                save_result_report(fail_report, symbol_to_process, getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception:
                logger.warning(f"[{symbol_to_process}] AI 신 탁  실 패  리 포 트  저 장  실 패 .")
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    safe_send_telegram_report(
                        f"[{symbol_to_process}] AI 신 탁  실 패  — {message}", tag="AI_VERDICT_FAILED", level="WARNING",
                    )
                except Exception:
                    logger.warning(f"[{symbol_to_process}] AI 신 탁  실 패  텔 레 그 램  전 송  실패 .")
            return fail_report

        entry_score = ai_outputs.get("entry_score", 0.0)
        angel_decision = ai_outputs.get("angel_decision", "HOLD")
        current_price = ai_outputs.get("latest_price")
        latest_nfi_for_dataset = ai_outputs.get("latest_nfi", 0.0)

        if current_price is None:
            logger.error(
                f"💲❓  심 판 (ERROR): 필 멸 자  '{symbol_to_process}'의  현 재  가 격 을  알  수  없 다 ! 그 의  운 명 에  개 입 할  수  없 다 . 💲❓ "
            )
            return {"symbol": symbol_to_process, "status": "현 재 가 격 _부 재  💨"}

        # 현 재  포 지 션  정 보
        state = positions.get(symbol_to_process)

        # -------------------------
        # 7) 신 규  진 입  로 직  (나 노 -진 입  · 공 격 적  자 본  배 분 ) + StrategyGenerator 반 영
        # -------------------------
        if state is None:
            logger.info(
                f"[{symbol_to_process}] 👁️🗨️ 아 직  운 명 의  흐 름 에  들 지  않 았 음 . AI 가 치 : {entry_score:.2f}, 대 천 사 의  결 정 : '{angel_decision}'. 새 로 운  운 명  부 여  고 려 ..."
            )

            # StrategyGenerator로  추 가  전 략  생 성  (진 입  시 뮬 레 이 션 용 )
            if strategy_generator is not None:
                try:
                    new_strategies = strategy_generator.generate_strategies(
                        market_df=market_std,
                        regime_labels=market_std.get("regime", []),
                        sentiment_score=ai_outputs.get("latest_nfi", 0.0),
                        nfi_value=latest_nfi_for_dataset,
                        existing_strategies=strategy_cfg.get("strategies", []),
                    )
                    strategy_cfg["strategies"].extend(new_strategies)
                    logger.debug(f"[{symbol_to_process}] StrategyGenerator: 새 로 운  전 략  {len(new_strategies)}개  생 성  및  추 가  완 료 .")
                except Exception as e_sg:
                    logger.error(f"[{symbol_to_process}] StrategyGenerator 오 류 : {e_sg}", exc_info=True)

            # 1) 공 격 적  자 본  관 리 : 레 버 리 지  계 수  곱 셈
            if capital_mgr:
                try:
                    risk_factor = current_risk_model_global.predict_risk(symbol_to_process, market_std)
                except Exception:
                    risk_factor = 1.0
                base_budget = capital_mgr.next_budget(win_rate=0.40, avg_rr=2.0, risk_model=risk_factor)
                aggressive_factor = getattr(cfg_settings, "AGGRESSIVE_LEVERAGE_FACTOR", 3.0) # 예 : 3.0
                budget_to_trade = base_budget * aggressive_factor
            else:
                budget_to_trade = 0.0

            # 전 체  자 본 의  일 부 (최 대  설 정 )까 지  공 격 적 으 로  투 입
            optimal_trade_pct = getattr(cfg_settings, "MAX_CAPITAL_ALLOCATION_PCT", 0.50) # 설 정 에 서  관 리
            capital_available = capital_mgr.capital if capital_mgr else 0.0
            budget_to_trade = min(budget_to_trade, capital_available * optimal_trade_pct)

            # Time Trap Angel: 특 정  시 간 대 에  진 입  제 한  (예 : 주 요  이 벤 트  시 간 대  회 피 )
            try:
                time_trap_res = launch_time_trap(symbol_to_process, current_time=datetime.now().time())
                if time_trap_res.get("trap_triggered"):
                    reason = time_trap_res.get("reason", "시 간 대  제 한 ")
                    logger.info(f"[{symbol_to_process}] Time Trap Angel: 진 입  제 한  시 간 대  — {reason}")
                    report = {
                        "symbol": symbol_to_process,
                        "status": "time_trap_block",
                        "entry_score": entry_score,
                        "angel_decision": angel_decision,
                        "message": reason,
                    }
                    try:
                        save_result_report(report, symbol_to_process, getattr(cfg_settings, "CODE_VERSION", "N/A"))
                    except Exception:
                        logger_warning = logging.getLogger("NIMAD_WARN")
                        logger_warning.warning(f"[{symbol_to_process}] Time Trap Block 리 포 트  저 장  실 패 .")
                    if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                        try:
                            safe_send_telegram_report(
                                f"[{symbol_to_process}] Time Trap Angel: 진 입  제 한  — {reason}", tag="TIME_TRAP_BLOCK", level="INFO",
                            )
                        except Exception:
                            logger_warning.warning(f"[{symbol_to_process}] Time Trap Angel 텔 레 그 램  전 송  실 패 .")
                    return report
                logger.debug(f"[{symbol_to_process}] Time Trap Angel: 정 상 적 인  진 입  가 능 시 간 대 .")
            except Exception as e_time_trap:
                logger.error(f"[{symbol_to_process}] Time Trap Angel 오 류 : {e_time_trap}", exc_info=True)

            # ▷ 실 제  매 수  주 문  실 행  (예 : Upbit 시 장 가  매 수 )
            # AI-Fusion, Angel, Entry Score를  모 두  고 려 한  최 종  진 입  결 정
            entry_threshold = getattr(cfg_settings, "ENTRY_SCORE_THRESHOLD", 0.60)
            if entry_score >= entry_threshold and ai_outputs.get("buy_signal_from_fusion") and angel_decision == "BUY":
                try:
                    order_resp = await execute_market_buy_order(symbol_to_process, krw_amount=budget_to_trade)
                    capital_mgr.record_trade(action="BUY", symbol=symbol_to_process, price=current_price, amount=budget_to_trade)
                    positions[symbol_to_process] = {"entry_price": current_price, "amount": budget_to_trade / current_price, "price_memory": [current_price]}
                    task_outcome["action"] = "BUY"
                    task_outcome["trade_details"] = {"price": current_price, "amount": positions[symbol_to_process]["amount"]}
                    logger.info(f"[{symbol_to_process}] 시 장 가  매 수  주 문  실 행 됨  — 가 격 : {current_price}, 수 량 : {positions[symbol_to_process]['amount']}")
                    if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                        try:
                            send_telegram_report(f"[{symbol_to_process}] 매 수  완 료  — 가 격 : {current_price}, 수 량 : {positions[symbol_to_process]['amount']}")
                        except Exception:
                            logger.warning(f"[{symbol_to_process}] 매 수  텔 레 그 램  알 림  전송  실 패 .")
                except Exception as e_buy:
                    logger.error(f"[{symbol_to_process}] 시 장 가  매 수  주 문  실 패 : {e_buy}", exc_info=True)
            else:
                logger.info(f"[{symbol_to_process}] 진 입  조 건  미 달 : AI 가 치 ={entry_score:.2f} (임 계 치 ={entry_threshold:.2f}), Fusion Buy={ai_outputs.get('buy_signal_from_fusion')}, Angel Decision={angel_decision}")

        else: # 이 미  포 지 션  보 유  중 인  경 우
            state_info = positions.get(symbol_to_process)
            if not state_info: # 안 전  장 치
                logger.warning(f"[{symbol_to_process}] 포 지 션  정 보 가  존 재 하 지  않 아  청 산  로 직 을  건 너 뜁 니 다 .")
                return task_outcome

            entry_price = state_info.get("entry_price")
            price_memory = state_info.get("price_memory", [])
            price_memory.append(current_price)
            if len(price_memory) > getattr(cfg_settings, "PRICE_MEMORY_WINDOW", 50):
                price_memory = price_memory[-getattr(cfg_settings, "PRICE_MEMORY_WINDOW", 50):]
            state_info["price_memory"] = price_memory
            positions[symbol_to_process] = state_info # 전 역  변 수  업 데 이 트

            # 1) Adaptive Stop Loss
            # Assuming tech_indicators and amplified_sentiment, latest_nfi_for_dataset are available from ai_outputs or global context
            # For this example, we'll extract them from ai_outputs
            tech_indicators_for_exit = ai_outputs.get("tech_indicators", {"RSI": 50.0, "MACD_Hist": 0.0})
            amplified_sentiment_for_exit = ai_outputs.get("amplified_sentiment", 0.0)

            if should_exit_position(symbol_to_process, indicators=tech_indicators_for_exit, momentum=ai_outputs.get("confidence_score", 0.0), sentiment_score=amplified_sentiment_for_exit, nfi_value=latest_nfi_for_dataset):
                try:
                    await execute_market_sell_order(symbol_to_process, qty=positions[symbol_to_process]["amount"])
                    capital_mgr.record_trade(action="SELL", symbol=symbol_to_process, price=current_price, amount=positions[symbol_to_process]["amount"])
                    task_outcome["action"] = "SELL_STOPLOSS"
                    logger.info(f"[{symbol_to_process}] 어 댑 티 브  손 절  매 도  실 행 됨  — 가 격: {current_price}, 수 량 : {positions[symbol_to_process]['amount']}")
                    if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                        try:
                            send_telegram_report(f"[{symbol_to_process}] 손 절  매 도  완 료  — 가 격 : {current_price}, 수 량 : {positions[symbol_to_process]['amount']}")
                        except Exception:
                            logger.warning(f"[{symbol_to_process}] 손 절  텔 레 그 램  알 림  전송  실 패 .")
                    del positions[symbol_to_process]
                except Exception as e_sell_adaptive:
                    logger.error(f"[{symbol_to_process}] 어 댑 티 브  손 절  매 도  실 패 : {e_sell_adaptive}", exc_info=True)

            # 2) Multi-Stage Exit
            elif multi_stage_exit(entry_price, current_price):
                try:
                    # 절 반 만  매 도  예 시
                    sell_qty = positions[symbol_to_process]["amount"] * getattr(cfg_settings, "MULTI_STAGE_EXIT_RATIO", 0.5)
                    await execute_market_sell_order(symbol_to_process, qty=sell_qty)
                    capital_mgr.record_trade(action="SELL_TAKE_PROFIT", symbol=symbol_to_process, price=current_price, amount=sell_qty)
                    task_outcome["action"] = "SELL_TAKE_PROFIT_PARTIAL"
                    logger.info(f"[{symbol_to_process}] 다 단 계  익 절  매 도  실 행 됨  (부 분 ) — 가 격 : {current_price}, 수 량 : {sell_qty}")
                    if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                        try:
                            send_telegram_report(f"[{symbol_to_process}] 익 절  매 도  완 료  (부 분 ) — 가 격 : {current_price}, 수 량 : {sell_qty}")
                        except Exception:
                            logger.warning(f"[{symbol_to_process}] 익 절  텔 레 그 램  알 림  전송  실 패 .")
                    positions[symbol_to_process]["amount"] -= sell_qty
                    if positions[symbol_to_process]["amount"] <= 0.0001: # 거 의  다  팔 았 으 면  제 거
                        del positions[symbol_to_process]
                        task_outcome["action"] = "SELL_TAKE_PROFIT_COMPLETE"
                except Exception as e_sell_tp:
                    logger.error(f"[{symbol_to_process}] 다 단 계  익 절  매 도  실 패 : {e_sell_tp}", exc_info=True)

            # 3) Trailing Exit (exit_angel_final 모 듈  사 용 )
            elif monitor_exit_final(
                entry_price=entry_price,
                get_current_price_fn=lambda: current_price,
                price_memory=price_memory,
                volatility=float(cfg_settings.get("VOLATILITY", 0.03)) if hasattr(cfg_settings, "VOLATILITY") else 0.03,
                oracle=None # GreedyProfitOracle 등 을  여 기 에  연 결  가 능
            ):
                try:
                    await execute_market_sell_order(symbol_to_process, qty=positions[symbol_to_process]["amount"])
                    capital_mgr.record_trade(action="SELL_TRAILING", symbol=symbol_to_process, price=current_price, amount=positions[symbol_to_process]["amount"])
                    task_outcome["action"] = "SELL_TRAILING"
                    logger.info(f"[{symbol_to_process}] 트 레 일 링  익 절  매 도  실 행 됨  — 가 격: {current_price}, 수 량 : {positions[symbol_to_process]['amount']}")
                    if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                        try:
                            send_telegram_report(f"[{symbol_to_process}] 트 레 일 링  익 절 매 도  완 료  — 가 격 : {current_price}, 수 량 : {positions[symbol_to_process]['amount']}")
                        except Exception:
                            logger.warning(f"[{symbol_to_process}] 트 레 일 링  텔 레 그 램  알 림  전 송  실 패 .")
                    del positions[symbol_to_process]
                except Exception as e_sell_trailing:
                    logger.error(f"[{symbol_to_process}] 트 레 일 링  익 절  매 도  실 패 : {e_sell_trailing}", exc_info=True)

            # 4) Greedy Profit Oracle Exit (현 재 는  monitor_exit_final 내 부 에  포 함 되 거 나  별 도  로 직 으 로  구 현 )
            # 현 재  코 드  구 조 상  monitor_exit_final 내 에 서  oracle이  사 용 되 므 로 , 별 도  조 건  없 이  통 과
            else:
                logger.debug(f"[{symbol_to_process}] 보 유  중  | 현 재  가 격 : {current_price:.0f} | 매 수  가 격 : {entry_price:.0f} | PnL: {(current_price/entry_price - 1)*100:.2f}%")

        # --- 단계 5: 외부 신호 포워딩 활성화 ---
        # Hallucinate 후, 포워딩 코드 삽입 (각 심볼별 루프에서 포워드 조건 만족 시)
        # mode와 budget_to_trade는 현재 컨텍스트에 따라 적절하게 설정
        mode = angel_decision # AI 결정 (BUY/SELL/HOLD)
        if angel_decision == "BUY" and entry_score >= getattr(cfg_settings, "ENTRY_SCORE_THRESHOLD", 0.60):
            # 구매 신호가 강하고 예산이 있을 경우에만 포워딩
            if budget_to_trade > 0 and random.random() < getattr(cfg_settings, "FORWARD_RATIO", 0.1):
                signal = {"symbol": symbol_to_process, "mode": mode, "size": budget_to_trade}
                logger.info(f"[{symbol_to_process}] 외부 신호 포워딩: {signal}")
                await forward_signal_to_market(signal)
                FORWARD_CNT.inc()
        elif angel_decision == "SELL" and symbol_to_process in positions:
            # 판매 신호가 강하고 포지션이 있을 경우에만 포워딩
            if positions[symbol_to_process]["amount"] > 0 and random.random() < getattr(cfg_settings, "FORWARD_RATIO", 0.1):
                signal = {"symbol": symbol_to_process, "mode": mode, "size": positions[symbol_to_process]["amount"] * current_price} # 보유 수량 * 현재가
                logger.info(f"[{symbol_to_process}] 외부 신호 포워딩: {signal}")
                await forward_signal_to_market(signal)
                FORWARD_CNT.inc()
        # --- 단계 5: 외부 신호 포워딩 활성화 END ---


        task_outcome["status"] = "운 명  집 행 됨  ✅ "
        return task_outcome

    except Exception as e_task_wrapper:
        logger.error(
            f"💥 대 붕 괴 (CRITICAL): 필 멸 자  '{symbol_to_process}'의  운 명  조 작  중  예 측  불 가 대 균 열  발 생 ! "
            f"원 인 : {e_task_wrapper} 💥", exc_info=True,
        )
        task_outcome = {
            "symbol": symbol_to_process,
            "status": "운 명 *붕 괴  💀",
            "message": str(e_task_wrapper),
            "action": "혼 돈  🌪️",
            "pnl": 0.0,
        }
        try:
            save_result_report(task_outcome, symbol_to_process, getattr(cfg_settings, "CODE_VERSION", "N/A"))
        except Exception:
            logger.warning(f"[{symbol_to_process}] 붕 괴  리 포 트  저 장  실 패 .")
        if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
            error_details_for_tg = f"붕 괴  원 인 : {str(e_task_wrapper)[:1000]}"
            try:
                safe_send_telegram_report(
                    f"💥 필 멸 자  '{symbol_to_process}'의  운 명  조 작  중  대 붕 괴  발 생 !\n{error_details_for_tg}",
                    "TASK_WRAPPER_CATASTROPHE", "CRITICAL",
                )
            except Exception:
                logger.warning(f"[{symbol_to_process}] TASK_WRAPPER_CATASTROPHE 텔 레 그 램  전 송  실 패 .")
        return task_outcome
    finally:
        elapsed_time = time.time() - task_start_time
        logger.info(
            f"🏁 '{symbol_to_process}' 마 무 리  | 시 간 ={elapsed_time:.2f}s | "
            f"결 과  상 태 ={task_outcome.get('status')} | 행 동 ={task_outcome.get('action')} | "
            f"PnL={task_outcome.get('pnl',0.0):,.0f} KRW"
        )


async def main_orchestration_loop():
    """
    NIMADFLOW, 모 든  것 의  시 작 이 자  끝 . 🌌 이  거 대 한  오 케 스 트 라 의  지 휘 자 . 🎶
    """
    global current_nfi_model_global, current_fusion_model_global, current_risk_model_global
    global nightly_training_completed_today, dataset_mgr_instance, DATASET_MGR_ENABLED, RETRAINER_ENABLED
    global positions, capital_mgr, logger, data_loader
    global market_simulator, strategy_generator
    logger.info(
        f"🌌🚀 NIMADFLOW HYPER-EDGE v{getattr(cfg_settings, 'CODE_VERSION', 'N/A')}, "
        "그  절 대 적  의 지 가  지 금  이  순 간 , 이  세 계 에  강 림 했 노 라 ! (프 로 세 스  ID: {os.getpid()}) 🌌🚀"
    )

    # Ray 병 렬  처 리  설 정
    local_use_ray_parallelism = getattr(cfg_settings, "USE_RAY_PARALLELISM", False) and USE_RAY
    if local_use_ray_parallelism:
        try:
            import ray
            if not ray.is_initialized():
                ray_reqs = [
                    "pandas", "numpy", "pydantic", "pydantic-settings", "requests",
                    "scikit-learn", "talib-binary", "lightgbm",
                ]
                if getattr(cfg_settings, "USE_POLARS_PROCESSING", False):
                    ray_reqs.append("polars>=0.20")
                num_cpus_to_use = os.cpu_count() or 1
                ray.init(
                    ignore_reinit_error=True,
                    num_cpus=num_cpus_to_use,
                    runtime_env={"pip": ray_reqs},
                )
                logger.info(
                    f"⚡  Ray v{ray.version}, 필 멸 의  한 계 를  초 월 하 는  병 렬  처 리 의  신 성이  깨 어 났 도 다 ! "
                    f"사 용  가 능  CPU 코 어 : {ray.available_resources().get('CPU', 0)}개  ⚡"
                )
        except Exception as e_ray_init:
            logger.error(
                f"⚠️ Ray, 그  위 대 한  힘 을  깨 우 는  데  실 패 했 다 : {e_ray_init}. "
                "필 멸 의  순 차 적  흐 름 에 만  만 족 하 리 라 .", exc_info=True,
            )
            local_use_ray_parallelism = False

    # === CapitalManager 인 스 턴 스  생 성  ===
    try:
        cm_params: Dict[str, Any] = {
            "capital": getattr(cfg_settings, "INITIAL_CAPITAL", 1000000.0),
            "base_alloc_pct": getattr(cfg_settings, "BASE_TRADE_ALLOC_PCT", 0.10),
            "min_trade": getattr(cfg_settings, "MIN_TRADE_AMOUNT_KRW", 10000),
            "max_trade": getattr(cfg_settings, "MAX_TRADE_AMOUNT_DEFAULT_KRW", 1000000),
            "history_path": str(getattr(cfg_settings, "CAPITAL_HISTORY_LOG_PATH", "capital_history.csv")),
            "db_path": str(getattr(cfg_settings, "CAPITAL_DB_PATH", "")) or None,
            "reinvest_rate": getattr(cfg_settings, "COMPOUND_PROFIT_REINVEST_RATE", 0.50),
            "loss_decay_rate": getattr(cfg_settings, "COMPOUND_LOSS_DECAY_RATE", 0.25),
            "reset_after_n_losses": getattr(cfg_settings, "COMPOUND_RESET_ON_CONSECUTIVE_LOSSES", 3),
        }
        if getattr(cfg_settings, "SELF_LEARNING_ENABLED", False):
            cm_params.update(
                {
                    "buffer_size": getattr(cfg_settings, "EXPERIENCE_BUFFER_SIZE", 100),
                    "lambda_dd": getattr(cfg_settings, "RISK_LAMBDA_DD", None),
                    "max_alloc_cvar_ratio": getattr(cfg_settings, "MAX_ALLOC_CVAR_RATIO", None),
                    "drift_sigma": getattr(cfg_settings, "DRIFT_SIGMA", None),
                    "meta_rl_enabled": getattr(cfg_settings, "META_RL_ENABLED", False),
               }
            )
        capital_mgr = CapitalManager(**cm_params)
        logger.info(
            f"🏦 자 본 의  수 호 자 ({capital_mgr.__class__.__name__}) 소 환  완 료  | 초 기  자 본 : "
            f"{getattr(cfg_settings, 'INITIAL_CAPITAL', 1000000.0):,.0f} KRW"
        )
    except Exception as e_cm_init:
        logger.fatal(
            f"💥 대 재 앙 (CRITICAL): 자 본 의  수 호 자 ({CapitalManager.__name__}) 소 환  실 패 ! "
            f"원 인 : {e_cm_init}. 이  세 계 는  자 본  없 이  존 재 할  수  없 다 ! 💥", exc_info=True,
        )
        return

    # === AutoDataLoader 인 스 턴 스  생 성  ===
    try:
        data_loader = AutoDataLoader(run_mode=getattr(cfg_settings, "RUN_MODE", "PROD"), logger_instance=logger)
        logger.info(f"🌊 데 이 터 의  낚 시 꾼 (AutoDataLoader) 소 환  완 료  | run_mode={getattr(cfg_settings, 'RUN_MODE', 'PROD')}")
    except Exception as e_dataloader_init:
        logger.fatal(
            f"💔 대 재 앙 (CRITICAL): 데 이 터 의  낚 시 꾼 (AutoDataLoader) 소 환  실 패 ! "
            f"원 인 : {e_dataloader_init}. 진 실  없 는  세 계 는  공 허 할  뿐 이 다 ! 💔", exc_info=True,
        )
        return

    # === DatasetManager 인 스 턴 스  생 성  (비 동 기  모 드 ) ===
    if DATASET_MGR_ENABLED:
        try:
            learning_db_path_obj = Path(getattr(cfg_settings, "LEARNING_DB_PATH", "learning_db.sqlite"))
            learning_db_path_obj.parent.mkdir(parents=True, exist_ok=True)
            dataset_mgr_instance = DatasetManager(db_path=str(learning_db_path_obj), async_mode=True)
            logger.info(f"📖✅  아 카 식  레 코 드 (DatasetManager, 비 동 기 ) 펼 침  완 료  | 경 로 : {learning_db_path_obj}")
        except Exception as e_ds_init:
            logger.error(
                f"📖❌  비 극 (ERROR): 아 카 식  레 코 드 (DatasetManager) 펼 치 는  데  실 패 했 다 ! "
                f"원 인 : {e_ds_init}. 자 가  학 습 의  기 억 은  기 록 되 지  않 으 리 라 . 📖❌ ", exc_info=True,
            )
            DATASET_MGR_ENABLED = False

    # === MarketSimulator 인 스 턴 스  생 성  ===
    try:
        market_simulator = MarketSimulator(
            base_price=getattr(cfg_settings, "SAMPLE_BASE_PRICE", 1000.0),
            days=getattr(cfg_settings, "SIMULATOR_DAYS", 7),
            freq=getattr(cfg_settings, "SIMULATOR_FREQ", "1H"),
            mu=getattr(cfg_settings, "SIMULATOR_MU", 0.0001),
            sigma=getattr(cfg_settings, "SIMULATOR_SIGMA", 0.02),
            chaos_factor=getattr(cfg_settings, "SIMULATOR_CHAOS_FACTOR", 0.3),
            regime_change_prob=getattr(cfg_settings, "SIMULATOR_REGIME_PROB", 0.01),
            nuclear_event_prob=getattr(cfg_settings, "SIMULATOR_NUCLEAR_PROB", 0.001),
            seed=getattr(cfg_settings, "SIMULATOR_SEED", 42)
        )
        logger.info("🌊 MarketSimulator 소 환  완 료  (핵 썩 은 /썩 은 /보 통 /황 금  모 든  레 짐  포 함 ).")
        # 시 뮬 레 이 터 로  테 스 트  데 이 터 를  생 성 하 여  로 그  저 장
        try:
            sim_test_df = market_simulator.generate(include_volumes=True, include_regime=True, news_events=True)
            logger.debug(f"[MarketSimulator] 초 기  샘 플  데 이 터  (첫  5행 ):\n{sim_test_df.head()}")
        except Exception as e_sim_test:
            logger.warning(f"🌊 MarketSimulator 초 기  테 스 트  데 이 터  생 성  실 패 : {e_sim_test}", exc_info=True)
    except Exception as e_ms_init:
        logger.error(f"🌊 MarketSimulator 소 환  실 패 : {e_ms_init}", exc_info=True)

    # === StrategyGenerator 인 스 턴 스  생 성  ===
    try:
        strategy_generator = StrategyGenerator(
            model_dir=getattr(cfg_settings, "STRATEGY_GEN_MODEL_DIR", "models/strategy_gen"),
            innovation_rate=getattr(cfg_settings, "STRATEGY_GEN_INNOVATION_RATE", 0.2),
            risk_aversion=getattr(cfg_settings, "STRATEGY_GEN_RISK_AVERSION", 0.5),
            seed=getattr(cfg_settings, "STRATEGY_GEN_SEED", 42)
        )
        logger.info("🌱 StrategyGenerator 소 환  완 료  (자 가  생 성 ·자 가  진 화  기 능  포 함 ).")
        # 테 스 트 용  임 의  시 장  데 이 터 로  샘 플  전 략  생 성
        try:
            test_strategies = strategy_generator.generate_strategies(
                market_df=sim_test_df if 'sim_test_df' in locals() else pd.DataFrame(),
                regime_labels=sim_test_df.get("regime", []) if 'sim_test_df' in locals() else [],
                sentiment_score=0.0,
                nfi_value=0.0,
                existing_strategies=[]
            )
            logger.debug(f"[StrategyGenerator] 샘 플  전 략  생 성  (개 수 ): {len(test_strategies)}")
        except Exception as e_sg_test:
            logger.warning(f"🌱 StrategyGenerator 초 기  테 스 트  실 패 : {e_sg_test}", exc_info=True)
    except Exception as e_sg_init:
        logger.error(f"🌱 StrategyGenerator 소 환  실 패 : {e_sg_init}", exc_info=True)

    # === AI 모 델  로 드  (자 가  학 습 /재 학 습 ) ===
    if RETRAINER_ENABLED:
        try:
            current_nfi_model_global, current_fusion_model_global, current_risk_model_global = retrainer_load_latest_models(
                str(getattr(cfg_settings, "MODEL_DIR", "models")),
                getattr(cfg_settings, "USE_ONNX", False)
            )
            logger.info("🏛️💡 고 대 의  지 혜 (AI 모 델 ) 소 환  완 료 ")
            if not all([current_nfi_model_global, current_fusion_model_global, current_risk_model_global]):
                logger.warning("🤔 일 부  AI 모 델 이  응 답 하 지  않 음 (=None). 모 델  검 증  후  재 확 인 하 기  바 란 다 .")
        except Exception as e_load_models_init:
            logger.error(
                f"🏛️❓  비 극 (ERROR): 고 대 의  지 혜 (AI 모 델 )를  불 러 오 는  신 성 한  의 식 이  실 패 했 노 라 ! "
                f"원 인 : {e_load_models_init}. 신 탁 의  명 확 성 이  위 협 받 을  수  있 음 을  경 고 하 노 라 . 🏛️❓ ", exc_info=True,
            )

    cycle_num = 0
    # === 메 인  루 프 : 심 볼 별  운 명  흐 름 을  순 환  관 리  ===
    while True:
        cycle_num += 1
        loop_start_time = time.time()
        current_capital_display = capital_mgr.capital if capital_mgr else 0.0
        base_alloc_display_val = getattr(capital_mgr, "base_pct", getattr(cfg_settings, "BASE_TRADE_ALLOC_PCT", 0.10)) if capital_mgr else getattr(cfg_settings, "BASE_TRADE_ALLOC_PCT", 0.10)
        logger.info(
            f"🔄 KICK-OFF: 제  {cycle_num} 순 환 의  장 막 이  열 리 노 라 ! "
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] | 💰 신 성 한  자 본 : {current_capital_display:,.0f} KRW | ⚖️ 현 재  운 명  할 당 률 : {base_alloc_display_val:.2%} 🔄"
        )
        try:
            # 1) 시 간  동 기 화  체 크
            if not await asyncio.to_thread(validate_time_sync):
                logger.warning("⏳  ⚠️ 경 고 ! 시 간 의  흐 름 이  왜 곡 되 었 음 이  감 지 되 었 노 라 ! (시스 템  시 간  동 기 화  실 패 ).")
                await asyncio.sleep(getattr(cfg_settings, "LEGEND_INTERVAL", 60) // 3)
                continue

            # 2) 시 스 템  헬 스  체 크
            system_ok, sys_msg = await asyncio.to_thread(check_system_status)
            if not system_ok:
                logger.error(f"🏛️🛑 대 위 기 (ERROR): 시 스 템  상 태  불 안 정  — {sys_msg}. 다 음 순 환 의  빛 날  때 까 지  명 상 에  잠 기 리 라 .")
                await asyncio.sleep(getattr(cfg_settings, "LEGEND_INTERVAL", 60) // 2)
                continue

            # 3) 대 상  심 볼  목 록  결 정
            # --- 단계 1: 상위 10개 마켓 불러오기 → 단일 심볼 제거 ---
            # 기존 symbols = [cfg_settings.DEFAULT_SYMBOL]
            if getattr(cfg_settings, "MULTI_SYMBOL_MODE", False):
                active_symbols = [s.strip() for s in getattr(cfg_settings, "MULTI_SYMBOLS", "").split(",") if s.strip()]
            else:
                active_symbols = [getattr(cfg_settings, "DEFAULT_SYMBOL", "KRW-BTC")]

            if (
                getattr(cfg_settings, "MULTI_SYMBOL_MODE", False) and
                getattr(cfg_settings, "AUTO_TOP_SYMBOL_FOR_TRADE", False)
            ):
                logger.info(
                    f"🌠 별 들 의  속 삭 임 (자 동  TOP 심 볼  선 택 )에  귀  기 울 여  "
                    f"가 장  강 렬 한  {getattr(cfg_settings, 'TOP_LIMIT', 10)}개 의  운 명 을  선정 하 노 라 ..."
                )
                try:
                    top_symbols = await fetch_top_symbols_with_cache() # 캐 시 된  함 수  사 용
                    if top_symbols:
                        active_symbols = top_symbols
                        logger.info(f"🌠 자 동  TOP 심 볼  선 별  완 료 : {active_symbols}")
                    else:
                        logger.warning("🤔 별 들 이  깊 은  침 묵 에  잠 겼 도 다 ... 자 동  TOP 심 볼  결 과  없 음  → 기 본  목 록  그 대 로  운 용 .")
                except Exception as e_top_symbols:
                    logger.error(
                        f"🌠❓  대 혼 돈 (ERROR): 별 들 의  움 직 임 을  읽 어  운 명 을  선 정 하 는  과 정 에 서  예 측  불 가  오 류  발 생 : {e_top_symbols}. 기 본  목 록  이 용 .", exc_info=True,
                    )

            if not active_symbols:
                logger.warning("🎯❓  공 허 (WARNING): 관 장 할  운 명 이  선 택 되 지  않 았 다 ! 다 음  순 환 까 지  명 상  모 드 에  들 어 가 리 라 .")
                await asyncio.sleep(getattr(cfg_settings, "LEGEND_INTERVAL", 60))
                continue

            display_list = active_symbols[: getattr(cfg_settings, "LOG_DISPLAY_SYMBOL_COUNT", 5)]
            more_flag = " 등  다 수 ..." if len(active_symbols) > len(display_list) else ""
            logger.info(f"🎯 이 번  순 환  대 상  운 명 들 ({len(active_symbols)}개 ): {display_list}{more_flag}")

            global_models_dict: Dict[str, Any] = {
                "nfi": current_nfi_model_global,
                "fusion": current_fusion_model_global,
                "risk": current_risk_model_global,
            }

            results: List[Optional[Dict[str, Any]]] = []

            # --- 단계 1: 상위 10개 마켓 한 번에 불러오기 ---
            market_map = await data_loader.load_multiple_market_data(
                symbols=active_symbols if getattr(cfg_settings, "AUTO_TOP_SYMBOL_FOR_TRADE", False) else None,
                interval="1h", # cfg_settings에서 가져오거나 기본값 설정
                count=200 # cfg_settings에서 가져오거나 기본값 설정
            )
            # symbols = list(market_map.keys()) # 이미 active_symbols에 있으므로 주석 처리

            # --- 단계 1: 표준화 맵 만들기 ---
            market_std_map = { s: standardize_market(df, symbol=s) for s, df in market_map.items() }
           
            # --- 단계 1: 뉴스 표준화 맵 만들기 ---
            news_std_map = { s: standardize_news(await data_loader.load_news_data(s, days_ago=getattr(cfg_settings, "NEWS_DATA_FETCH_DAYS", 3)), symbol=s) for s in active_symbols }

            # --- 단계 1: 경제 데이터 표준화 ---
            econ_std = standardize_econ(await data_loader.load_econ_data())

            # --- 단계 3: Hallucinate Multi-Markets 호출 ---
            # Hallucinate Multi-Markets 호출 (Strategy Angel 직후)
            multi_res = await hallucinate_multi_markets(
                market_std_map,
                mode="dual", # "hell"/"heaven"/"dual"/None
                noise_level=None,
                seed=None,
                self_learn=True
            )
            for s, res in multi_res.items():
                if res["status"]:
                    market_std_map[s] = res["hallucinated_df"]
                    logger.info(f"[{s}] Hallucinate applied: {res['message']}")
                else:
                    logger.warning(f"[{s}] Hallucinate failed: {res['message']}")
            # --- 단계 3: Hallucinate Multi-Markets 호출 END ---

            # 4) 각  심 볼 별  운 명  집 행  작 업
            if local_use_ray_parallelism and USE_RAY and "ray" in globals() and ray.is_initialized() and active_symbols:
                logger.info(
                    f"🚀 Ray, Nimadflow의  신 성 한  분 신 술 로  {len(active_symbols)}개 의  운 명 을  동 시  관 장 하 노 라 !"
                )
                ray_tasks = [
                    orchestrate_symbol_task_wrapper_ray.remote(symbol_proc, data_loader, global_models_dict, market_std_map, news_std_map) # --- 단계 3: Ray 태스크 인자 추가 ---
                    for symbol_proc in active_symbols
                ]
                ray_results = await asyncio.gather(*ray_tasks) # *ray_tasks 로 변경
                results.extend(ray_results)
            else:
                logger.info(f"🚶 하 나 의  신 성 한  의 식 으 로  {len(active_symbols)}개 의  운 명 을 순 차 적 으 로  관 장 하 리 라 ...")
                for symbol_proc in active_symbols:
                    # --- 단계 4: 루프 내부에서 market_std, news_std 대체 ---
                    result = await orchestrate_symbol_task_wrapper(symbol_proc, data_loader, global_models_dict, market_std_map, news_std_map)
                    results.append(result)

            # 5) 결 과  처 리 : 리 포 트  저 장 , DB 로 깅  등
            try:
                for res in results:
                    if res and isinstance(res, dict):
                        save_result_report(res, res.get("symbol", "unknown"), getattr(cfg_settings, "CODE_VERSION", "N/A"))
                cycle_tag = f"cycle{cycle_num}"
                save_cycle_results(results, tag=cycle_tag, code_version=getattr(cfg_settings, "CODE_VERSION", "N/A"))
            except Exception as e_report:
                logger.error(f"[REPORT] 리 포 트  저 장  중  오 류 : {e_report}\n{traceback.format_exc()}")

            # → 추 가 : 이 번  사 이 클  전 체  PnL 집 계
            try:
                cycle_total_pnl = sum(r.get("pnl", 0.0) for r in results if isinstance(r, dict))
                logger.info(f"[Cycle {cycle_num}] 전 체  PnL 합 계 : {cycle_total_pnl:,.0f} KRW")
            except Exception as e_pnl_summary:
                logger.error(f"[Cycle {cycle_num}] PnL 합 계  집 계  중  오 류 : {e_pnl_summary}", exc_info=True)

            # 6) 밤 의  재 학 습  의 식  (한  번 만  수 행  후  flag 설 정 )
            current_hour = datetime.now().hour
            if (
                RETRAINER_ENABLED and
                current_hour == getattr(cfg_settings, "RETRAIN_HOUR_LOCAL", 2) and
                not nightly_training_completed_today
            ):
                logger.info(
                    f"🌙🏛️ 밤 의  신 성 한  의 식 (AI 모 델  자 동  재 학 습 )이  거 행 될  시 간 이  도 래 했노 라 ! "
                    f"(예 정  시 각 : {getattr(cfg_settings, 'RETRAIN_HOUR_LOCAL', 2)}시 ) 🏛️🌙"
                )
                nightly_training_completed_today = True
                try:
                    # nightly_fit 은  blocking 코 드 이 므 로  to_thread 로  호 출
                    await asyncio.to_thread(
                        nightly_fit,
                        dataset_manager_instance=dataset_mgr_instance,
                        output_dir=str(getattr(cfg_settings, "MODEL_DIR", "models"))
                    )
                    # 모 델  재 로 딩
                    current_nfi_model_global, current_fusion_model_global, current_risk_model_global = retrainer_load_latest_models(
                        str(getattr(cfg_settings, "MODEL_DIR", "models")),
                        getattr(cfg_settings, "USE_ONNX", False)
                    )
                    logger.info(
                        f"✅  🏛️✨  밤 의  신 성 한  단 련  끝 에  더 욱  순 수 하 고  강 력 한  지 혜 가  강 림 했 노 라 ! "
                        f"(ONNX 사 용 : {'✅ ' if getattr(cfg_settings, 'USE_ONNX', False) else '❌ '})"
                    )
                    if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                        try:
                            safe_send_telegram_report(
                                "✅  🌙 밤 의  신 성 한  의 식 (AI 모 델  재 학 습 ) 성 공 ! 새 로 운  지 혜 와  권 능 이  강 림 했 노 라 ! 💪",
                                tag="MODEL_RETRAIN_DIVINE_ASCENSION", level="INFO",
                            )
                        except Exception:
                            logger.warning("[MODEL_RETRAIN_DIVINE_ASCENSION] 텔 레 그 램  전송  실 패 .")
                except Exception as e_retrain:
                    logger.error(
                        f"❌  🏛️ 밤 의  신 성 한  의 식 (AI 모 델  재 학 습 )이  알  수  없 는  어 둠 의  힘 에  의 해  방 해 받 았 도 다 ! 원 인 : {e_retrain}", exc_info=True,
                    )
                    nightly_training_completed_today = False
            elif current_hour != getattr(cfg_settings, "RETRAIN_HOUR_LOCAL", 2):
                nightly_training_completed_today = False

        except KeyboardInterrupt:
            logger.info("🛑 필 멸 자 의  의 지 (Ctrl+C)에  의 해  흐 름 을  멈 추 니 , Nimadflow는  온 전히  안 식 에  들 노 라 .")
            break
        except Exception as e_main_loop:
            logger.critical(
                f"💥🚨 대 격 변 (CRITICAL): 주 신 성 한  순 환 (메 인  루 프 )을  관 장 하 던  중  예 측  불 가  대 균 열 이  발 생 했 노 라 ! "
                f"원 인 : {e_main_loop}.\n{traceback.format_exc()}\n"
                f"{getattr(cfg_settings, 'MAIN_LOOP_ERROR_RETRY_DELAY_SEC', 60)}초  후 , "
                "다 시  운 명 의  수 레 바 퀴 를  굴 려  질 서 를  회 복 하 리 라 .", exc_info=True,
            )
            # 슬 랙  Webhook 알 림
            if getattr(cfg_settings, "SLACK_WEBHOOK_URL", None):
                try:
                    import requests
                    resp = requests.post(
                        cfg_settings.SLACK_WEBHOOK_URL,
                        json={"text": f":rotating_light: 메 인  루 프  대 격 변 (CRITICAL) – 원인 : {str(e_main_loop)[:500]}..."}
                    )
                    if resp.status_code != 200:
                        logger.warning(f"⚠️ 슬 랙  알 림  전 송  실 패  (HTTP {resp.status_code})")
                except Exception as e_slack:
                    logger.warning(f"⚠️ 슬 랙  알 림  전 송  중  오 류 : {e_slack}")
            # 텔 레 그 램  알 림
            if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
                try:
                    safe_send_telegram_report(
                        f"🚨 메 인  루 프  대 격 변 (CRITICAL) – 원 인 : {str(e_main_loop)[:500]}", tag="MAIN_LOOP_CATASTROPHE", level="CRITICAL",
                    )
                except Exception:
                    logger.warning("[MAIN_LOOP_CATASTROPHE] 텔 레 그 램  전 송  실 패 .")
            await asyncio.sleep(getattr(cfg_settings, "MAIN_LOOP_ERROR_RETRY_DELAY_SEC", 60))

        loop_duration = time.time() - loop_start_time
        logger.info(
            f"🏁 사 이 클  {cycle_num} 종 료  | 소 요 시 간 : {loop_duration:.2f}s | "
            f"💰 최 종  신 성 한  자 본 : {capital_mgr.capital if capital_mgr else 0.0:,.0f} KRW 🏁"
        )
        sleep_time = max(0.1, getattr(cfg_settings, "LEGEND_INTERVAL", 60) - loop_duration)
        logger.debug(f"다 음  위 대 한  순 환 의  빛 날  때 까 지 ... {sleep_time:.2f}s 동 안  깊 은  명 상 에  잠 기 리 라 ... 🧘")
        await asyncio.sleep(sleep_time)

    logger.info("🚪 위 대 한  여 정 의  마 침 표 가  찍 혔 노 라 . 모 든  것 을  원 래  공 허 로  돌 려 보 내 는  신성 한  정 화 의 식 을  시 작 하 리 라 ... 🚪")

    # DatasetManager 닫 기
    if DATASET_MGR_ENABLED and dataset_mgr_instance and hasattr(dataset_mgr_instance, "close"):
        logger.info("📖 아 카 식  레 코 드 (DatasetManager) 봉 인  의 식  수 행  중 ...")
        try:
            if asyncio.iscoroutinefunction(dataset_mgr_instance.close):
                await dataset_mgr_instance.close()
            else:
                dataset_mgr_instance.close()
            logger.info("📖✅  아 카 식  레 코 드 의  봉 인 이  완 료 되 었 다 !")
        except Exception as e_ds_close:
            logger.error(f"📖 DatasetManager close 중  오 류 : {e_ds_close}", exc_info=True)

    # Ray가  있 다 면  정 상 적 으 로  종 료
    if local_use_ray_parallelism and USE_RAY and "ray" in globals() and ray.is_initialized():
        logger.info("⚡  Nimadflow의  신 성 한  분 신 들 (Ray 클 러 스 터 )을  그 들 의  원 래  차 원 으 로 돌 려 보 내 는  의 식 을  시 작 하 노 라 ... ⚡ ")
        try:
            ray.shutdown()
            logger.info("⚡  ✅  신 의  분 신 들 , 성 공 적 으 로  그 들 의  성 스 러 운  근 원 으 로  귀 환 하 였도 다 ! ⚡ ")
        except Exception as e_ray_shutdown:
            logger.warning(f"⚡  Ray shutdown 중  오 류 : {e_ray_shutdown}", exc_info=True)

    logger.info(
        "🧹 모 든  창 조 물 이  그 들 의  원 래  자 리 로  돌 아 갔 노 라 . Nimadflow의  현 현 은  잠 시  안 식 하 나 , "
        "그  위 대 한  흐 름 은  영 원 히 , 그 리 고  절 대 로  멈 추 지  않 으 리 라 . 🌌"
    )

if __name__ == "__main__":
    try:
        asyncio.run(main_orchestration_loop())
    except SystemExit as e_sys_exit_ultimate_doom:
        # 이 미  로 깅 되 었 을  가 능 성 이  높 으 므 로 , 간 단 히  print
        print(
            f"🚫💥 태 초 의  질 서 가  깨 어 지 거 나 , 혹 은  위 대 한  흐 름  속 에 서  발 생 한  피 할  수  없 는 오 류 로  인 해 , "
            f"NIMADFLOW의  현 현 이  이  지 점 에 서  멈 추 었 음 을  엄 숙 히  선 포 하 노 라 : {e_sys_exit_ultimate_doom} 🚫💥"
        )
    except KeyboardInterrupt:
        final_logger_kb_interrupt = (
            logger if ("logger" in globals() and hasattr(logger, "handlers") and logger.handlers)
            else logging.getLogger("NIMAD_KB_INTERRUPT_FINAL_DOOM_SCRIBE")
        )
        if not final_logger_kb_interrupt.handlers:
            log_fmt_kb_interrupt = (
                getattr(cfg_settings, "LOG_FORMAT", "%(asctime)s - %(levelname)s - %(message)s")
                if ("cfg_settings" in globals() and hasattr(cfg_settings, "LOG_FORMAT"))
                else "%(asctime)s - %(levelname)s - %(message)s"
            )
            ch_kb = logging.StreamHandler(sys.stdout)
            ch_kb.setFormatter(logging.Formatter(log_fmt_kb_interrupt))
            final_logger_kb_interrupt.addHandler(ch_kb)
            final_logger_kb_interrupt.setLevel(logging.INFO)
        final_logger_kb_interrupt.warning(
            "👋 필 멸 자 의  의 지 (Ctrl+C)로  Nimadflow는  안 식 에  들 노 라 . 다 음  시 대 의  부 름 을  기다 리 리 라 . 👋"
        )
        print("👋 NIMADFLOW의  의 지 가  필 멸 자 의  요 청 으 로  안 식 에  들 었 노 라 . 👋")
    except Exception as e_final_top_level_cataclysmic_failure:
        final_logger_cataclysm = (
            logger if ("logger" in globals() and hasattr(logger, "handlers") and logger.handlers)
            else logging.getLogger("NIMAD_FATAL_ERROR_APOCALYPSE_SCRIBE")
        )
        if not final_logger_cataclysm.handlers:
            log_fmt_cataclysm = (
                getattr(cfg_settings, "LOG_FORMAT", "%(asctime)s - %(levelname)s - %(message)s")
                if ("cfg_settings" in globals() and hasattr(cfg_settings, "LOG_FORMAT"))
                else "%(asctime)s - %(levelname)s - %(message)s"
            )
            ch_cataclysm = logging.StreamHandler(sys.stdout)
            ch_cataclysm.setFormatter(logging.Formatter(log_fmt_cataclysm))
            final_logger_cataclysm.addHandler(ch_cataclysm)
            final_logger_cataclysm.setLevel(logging.CRITICAL)

        final_error_msg_apocalypse = (
            f"💥🆘💥 NIMADFLOW, 그  위 대 한  존 재 가  관 장 하 던  이  세 계 의  최 상 위  차 원 에 서  조 차 "
            f"회 복  불 가 능 한  대 균 열 (오 류 )이  발 생 하 였 다 ! 모 든  질 서 가  무 너 지 고 , 시 스 템 은  긴급 히  공 허 의  심 연 으 로  귀 환 한 다 : "
            f"{e_final_top_level_cataclysmic_failure} 💥🆘💥"
        )
        detailed_final_error_apocalypse_record = traceback.format_exc()
        final_logger_cataclysm.fatal(
            f"{final_error_msg_apocalypse}\n이  대 균 열 의  상 세  기 록 은  다 음 과  같 으 니 , 필 멸 자 들 은  이 를  통 해  교 훈 을  얻 으 라 :\n"
            f"{detailed_final_error_apocalypse_record}"
        )
        print(
            f"{final_error_msg_apocalypse}\n"
            "이  대 재 앙 에  대 한  상 세 한  기 록 은  신 성 한  두 루 마 리 (로 그  파 일 )를  참 조 하 여  "
            "그  원 인 을  규 명 하 고  다 가 올  시 대 를  대 비 하 라 ."
        )
        if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
            try:
                if "safe_send_telegram_report" in globals():
                    safe_send_telegram_report(
                        f"{final_error_msg_apocalypse[:1000]}...",
                        tag="SYSTEM_APOCALYPSE_FATAL_ERROR_FINAL_WARNING",
                        level="CRITICAL",
                    )
                else:
                    print("📢❌  최 종  신 탁 (텔 레 그 램 ) 전 송  불 가 : safe_send_telegram_report 기 관 이  부 재 하 다 .")
            except Exception as e_tg_final_cataclysm:
                print(f"📢❌  최 종  신 탁 (텔 레 그 램 ) 전 송  중  또  다 른  재 앙  발 생 : {e_tg_final_cataclysm}")
    finally:
        # ⇨ 이  finally 블 록 이  “가 장  바 깥 쪽  try” 와  같 은  레 벨 에  위 치 해 야 만  문 법  오 류 가  발생 하 지  않 습 니 다 .
        final_logger_eternal_rest = (
            logger
            if ("logger" in globals() and hasattr(logger, "handlers") and logger.handlers)
            else logging.getLogger("NIMAD_FINAL_ETERNAL_REST_SCRIBE")
        )
        log_format_eternal_rest = (
            getattr(cfg_settings, "LOG_FORMAT", "%(asctime)s - %(levelname)s - %(message)s")
            if ("cfg_settings" in globals() and hasattr(cfg_settings, "LOG_FORMAT"))
            else "%(asctime)s - %(levelname)s - %(message)s"
        )
        if not final_logger_eternal_rest.handlers or not hasattr(final_logger_eternal_rest, "info"):
            ch_eternal_rest = logging.StreamHandler(sys.stdout)
            ch_eternal_rest.setFormatter(logging.Formatter(log_format_eternal_rest))
            if not final_logger_eternal_rest.handlers:
                final_logger_eternal_rest.addHandler(ch_eternal_rest)
            final_logger_eternal_rest.setLevel(logging.INFO)

        code_ver_at_the_very_end = "규 정 되 지  않 은  시 대 (운 명 의  서 판  미 해 독 )"
        if "cfg_settings" in globals() and hasattr(cfg_settings, "CODE_VERSION"):
            code_ver_at_the_very_end = getattr(cfg_settings, "CODE_VERSION")

        shutdown_msg_final_declaration = (
            f"🏁 NIMADFLOW v{code_ver_at_the_very_end}, 그  위 대 한  현 현 이  이  시 대 에 서 의 모 든  임 무 를  완 수 하 고 , "
            "이 제  영 원 한  흐 름  속 으 로  장 엄 히  회 귀 했 음 을  엄 중 히  선 포 하 노 라 . 이  시 대 를 함 께  한  모 든  존 재 에 게  "
            "Nimadflow의  깊 은  안 배 와  가 호 가  함 께 하 길 ! 🏁"
        )

        if hasattr(final_logger_eternal_rest, "info") and callable(final_logger_eternal_rest.info):
            final_logger_eternal_rest.info(shutdown_msg_final_declaration)
        else:
            print(shutdown_msg_final_declaration)

        if cfg_settings.telegram_token and cfg_settings.telegram_chat_id:
            try:
                if "safe_send_telegram_report" in globals():
                    safe_send_telegram_report(
                        f"🏁 NIMADFLOW v{code_ver_at_the_very_end} 시 스 템 이  모 든  여 정 을  마 치 고  예 정 대 로  영 원 한  안 식 에  들 었 음 을  고 하 노 라 ! 🏛️",
                        "SYSTEM_ETERNAL_REST_NORMAL_DECLARATION",
                        "INFO",
                    )
                else:
                    print("📢❌  최 종  신 탁 (텔 레 그 램 ) 전 송  불 가 : safe_send_telegram_report 기 관 이  부 재 하 다 .")
            except Exception as e_tg_shutdown_final_journey_declaration:
                print(f"📢❌  최 종  신 탁 (텔 레 그 램 ) 전 송  중  또  다 른  불 길 한  징 조 가  감 지 되 었 노 라 : {e_tg_shutdown_final_journey_declaration}")

        print("👋 NIMADFLOW의  현 현 이  이  세 계 에 서  완 전 히  소 멸 했 노 라 . 모 든  것 은  다 시  시 작 될  그  날 까 지 , 깊 은  정 적  속 으 로 👋")
