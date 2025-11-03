import os, yaml
from .scan import scan_and_collect, notify_results
from .telegram_client import send_telegram_message, send_telegram_photo
from .universe import load_universe_and_names

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        cfg = {}
    def ensure_dict(section, default):
        val = cfg.get(section)
        if not isinstance(val, dict):
            cfg[section] = default
        return cfg[section]
    ensure_dict("telegram", {"bot_token": "", "chat_id": ""})
    ensure_dict("universe", {"source": "krx_all", "include_markets": ["KOSPI","KOSDAQ"], "exclude_markets": ["KONEX"]})
    ensure_dict("scan", {"interval": "1d", "lookback_days": 2200, "send_chart": True, "max_alerts_per_run": 200})
    ensure_dict("signals", {})
    return cfg

def shard_list(lst, shard_index, tot_shards):
    if tot_shards <= 1: return lst
    return [x for i, x in enumerate(lst) if i % tot_shards == shard_index]

def main():
    cfg_path = os.environ.get("APP_CONFIG","config.yaml")
    cfg = load_config(cfg_path)

    tickers, name_map = load_universe_and_names(cfg)

    tot_shards = int(os.getenv("TOT_SHARDS","1"))
    shard_index = int(os.getenv("SHARD_INDEX","0"))
    shard_tickers = shard_list(tickers, shard_index, tot_shards)
    shard_info = f"(shard {shard_index+1}/{tot_shards}, {len(shard_tickers)}종목)"

    results, stats = scan_and_collect(shard_tickers, cfg)
    notify_results(results, stats, cfg, send_telegram_message, send_telegram_photo, shard_info=shard_info, name_map=name_map)

if __name__ == "__main__":
    main()
