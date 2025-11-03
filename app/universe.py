import FinanceDataReader as fdr

def _suffix_for_market(market):
    m = str(market).upper()
    if m.startswith("KOSPI"): return ".KS"
    if m.startswith("KOSDAQ"): return ".KQ"
    return None

def load_universe_and_names(cfg):
    uni = (cfg or {}).get("universe") or {}
    if not isinstance(uni, dict):
        uni = {"source": "krx_all", "include_markets": ["KOSPI","KOSDAQ"], "exclude_markets": ["KONEX"]}
    src = str(uni.get("source", "krx_all")).lower()

    tickers = []
    name_map = {}

    def _add(code, market, name):
        suffix = _suffix_for_market(market)
        if not suffix:
            return
        t = f"{code}{suffix}"
        tickers.append(t)
        name_map[code] = name
        name_map[t] = name

    if src == "krx_all":
        include = set([m.upper() for m in uni.get("include_markets", ["KOSPI","KOSDAQ"])])
        exclude = set([m.upper() for m in uni.get("exclude_markets", [])])
        df = fdr.StockListing("KRX")
        if df is None or df.empty:
            raise RuntimeError("FDR에서 KRX 상장사 목록을 불러오지 못했습니다.")
        for _, r in df.iterrows():
            market = str(r.get("Market","")).upper()
            if market in exclude or market not in include:
                continue
            code = str(r["Code"])
            name = str(r.get("Name", code))
            _add(code, market, name)

    elif src == "krx_index":
        idx = uni.get("krx_index", "KOSPI200")
        df = fdr.StockListing(idx)
        if df is None or df.empty:
            raise RuntimeError(f"FDR에서 {idx} 구성 종목을 불러오지 못했습니다.")
        for _, r in df.iterrows():
            code = str(r["Code"])
            market = str(r.get("Market","KOSPI"))
            name = str(r.get("Name", code))
            _add(code, market, name)

    elif src == "list":
        base = fdr.StockListing("KRX")
        base_map = {}
        if base is not None and not base.empty:
            for _, r in base.iterrows():
                base_map[str(r["Code"])] = str(r.get("Name", r["Code"]))
        for t in list(uni.get("tickers", [])):
            t = str(t)
            tickers.append(t)
            code = t.split(".")[0]
            if code in base_map:
                name_map[code] = base_map[code]
                name_map[t] = base_map[code]

    tickers = sorted(set(tickers))
    return tickers, name_map

def load_universe(cfg):
    return load_universe_and_names(cfg)[0]
