"""Velas publicas de 1h da Binance: um arquivo por par e por mes fechado, em
dados/<PAR>/<AAAA-MM>.csv.gz. So dados publicos de mercado: sem conta, sem chave e sem ordens.

Pares incluidos:
  1. os listados em pares.txt;
  2. todo par cotado em USDT que ja esteve entre os 30 de maior volume em algum mes fechado,
     contado a partir do quarto mes de negociacao do par. Ficam de fora moedas estaveis, moedas
     fiduciarias, versoes "embrulhadas" e tokens alavancados.

Cada execucao busca so os meses fechados que faltam; arquivo gravado nao e alterado.
Colunas: abertura_ms (inicio da vela, UTC, em milissegundos), open, high, low, close,
volume (moeda base) e volume_cotado (USDT). A tabela usada na selecao automatica fica em
universo/volume_mensal.csv.gz e a situacao de cada par em manifesto.json.
"""
import csv, gzip, io, json, os, re, time, zipfile
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

API = "https://data-api.binance.vision/api/v3"
ARQUIVO = "https://data.binance.vision/data/spot/monthly/klines"
HORA = 3_600_000
PRIMEIRO_MES = (2018, 1)
TOPO = 30        # posicoes do ranking mensal de volume que entram na selecao
CARENCIA = 3     # primeiros meses de cada par, ignorados no ranking
FILAS = 6
COLUNAS = ["abertura_ms", "open", "high", "low", "close", "volume", "volume_cotado"]
FORA = set("""USDC BUSD TUSD USDP PAX FDUSD DAI USDS USDSB SUSD UST USTC USDE PYUSD XUSD USD1
BFUSD EUR EURI AEUR GBP AUD BRL TRY RUB UAH NGN ZAR PLN RON ARS MXN COP CZK JPY IDRT BIDR
BVND BKRW VAI PAXG WBTC WBETH BETH BNSOL""".split())


def ms(ano, mes):
    return int(datetime(ano, mes, 1, tzinfo=timezone.utc).timestamp() * 1000)


def seguinte(ano, mes):
    return (ano + 1, 1) if mes == 12 else (ano, mes + 1)


def mes_de(t):
    d = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
    return d.year, d.month


def iso(t):
    return datetime.fromtimestamp(int(t) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


def enxuto(valor):
    return valor.rstrip("0").rstrip(".") if "." in valor else valor


def linha(k):
    return [int(k[0]), *(enxuto(str(v)) for v in k[1:6]), enxuto(str(k[7]))]


def baixar(url, ausente=(404,), espera=30):
    """Conteudo da URL, ou None quando o codigo da resposta esta em ausente."""
    pedido = urllib.request.Request(url, headers={"User-Agent": "cryptotrade-dados/1.0"})
    for tentativa in range(6):
        try:
            with urllib.request.urlopen(pedido, timeout=espera) as resposta:
                return resposta.read()
        except urllib.error.HTTPError as erro:
            if erro.code in ausente:
                return None
            time.sleep(min(max(2 ** tentativa * 2, 30 if erro.code in (418, 429) else 0), 120))
        except Exception:
            time.sleep(2 ** tentativa)
    raise RuntimeError("sem resposta de " + url.split("?")[0])


def velas(par, intervalo, inicio, fim):
    """Ate 1000 velas com abertura em [inicio, fim). None quando a API nao conhece o par."""
    consulta = urllib.parse.urlencode({"symbol": par, "interval": intervalo, "startTime": inicio,
                                       "endTime": fim - 1, "limit": 1000})
    corpo = baixar(API + "/klines?" + consulta, ausente=(400,))
    time.sleep(0.1)
    return None if corpo is None else json.loads(corpo)


def do_arquivo(par, ano, mes):
    """Velas de um mes no arquivo publico da Binance (para pares que sairam da corretora)."""
    corpo = baixar(f"{ARQUIVO}/{par}/1h/{par}-1h-{ano}-{mes:02d}.zip", ausente=(403, 404), espera=60)
    if corpo is None:
        return []
    with zipfile.ZipFile(io.BytesIO(corpo)) as pacote:
        texto = pacote.read(pacote.namelist()[0]).decode()
    saida = []
    for k in csv.reader(io.StringIO(texto)):
        if k and k[0].isdigit():                       # pula cabecalho, se houver
            t = int(k[0])
            saida.append([t // 1000 if t > 10 ** 14 else t, *k[1:]])   # desde 2025: microssegundos
    return saida


def gravar(caminho, cabecalho, linhas):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    texto = io.StringIO()
    escritor = csv.writer(texto, lineterminator="\n")
    escritor.writerow(cabecalho)
    escritor.writerows(linhas)
    with open(caminho, "wb") as arquivo:               # mtime=0: mesmo conteudo, mesmo arquivo
        arquivo.write(gzip.compress(texto.getvalue().encode(), mtime=0))


def ler(caminho):
    with gzip.open(caminho, "rt", newline="") as arquivo:
        return list(csv.reader(arquivo))[1:]


def atualizar(par, fim):
    """Grava os meses fechados que faltam para um par e devolve a situacao dele."""
    pasta = os.path.join("dados", par)
    feitos = sorted(a[:7] for a in os.listdir(pasta)) if os.path.isdir(pasta) else []
    mes = seguinte(int(feitos[-1][:4]), int(feitos[-1][5:7])) if feitos else PRIMEIRO_MES
    cursor, por_mes, origem, erro = ms(*mes), {}, "api", None
    try:
        while cursor < fim:
            lote = velas(par, "1h", cursor, fim)
            if lote is None:
                origem = "arquivo"
                break
            if not lote:
                break
            for k in lote:
                por_mes.setdefault(mes_de(int(k[0])), []).append(linha(k))
            cursor = int(lote[-1][0]) + HORA
        while origem == "arquivo" and ms(*mes) < fim:
            lote = do_arquivo(par, *mes)
            if lote:
                por_mes[mes] = [linha(k) for k in lote]
            mes = seguinte(*mes)
            cursor = ms(*mes)
    except Exception as falha:                         # guarda o que ja esta completo
        erro = str(falha)
        por_mes = {m: v for m, v in por_mes.items() if ms(*seguinte(*m)) <= cursor}
    for (ano, m), linhas in por_mes.items():
        gravar(os.path.join(pasta, f"{ano}-{m:02d}.csv.gz"), COLUNAS, linhas)
    feitos = sorted(os.listdir(pasta)) if os.path.isdir(pasta) else []
    if not feitos:
        return {"status": "erro: " + erro if erro else "sem dados"}
    return {"status": "parcial: " + erro if erro else "ok", "origem": origem, "meses": len(feitos),
            "meses_novos": len(por_mes),
            "primeira_vela": iso(ler(os.path.join(pasta, feitos[0]))[0][0]),
            "ultima_vela": iso(ler(os.path.join(pasta, feitos[-1]))[-1][0])}


def universo(fim):
    """Pares com USDT que ja estiveram no topo do ranking mensal de volume."""
    simbolos = json.loads(baixar(API + "/exchangeInfo", espera=120))["symbols"]
    bases = {s["symbol"]: s["baseAsset"] for s in simbolos
             if s.get("quoteAsset") == "USDT" and re.fullmatch("[A-Z0-9]+", s["symbol"])}
    todas = set(bases.values())

    def serve(base):
        if base in FORA or base.endswith(("DOWN", "BULL", "BEAR")):
            return False
        return not (base.endswith("UP") and base[:-2] + "DOWN" in todas)

    candidatos = sorted(par for par, base in bases.items() if serve(base))

    def mensal(par):
        try:
            return par, [(int(k[0]), float(k[7])) for k in velas(par, "1M", ms(2017, 1), fim) or []]
        except Exception:
            return par, None

    with ThreadPoolExecutor(FILAS) as fila:
        tabela = list(fila.map(mensal, candidatos))
    gravar("universo/volume_mensal.csv.gz", ["par", "mes", "volume_cotado"],
           [[par, iso(t)[:7], round(v)] for par, meses in tabela for t, v in meses or []])
    ranking = {}
    for par, meses in tabela:
        for t, v in (meses or [])[CARENCIA:]:
            ranking.setdefault(t, []).append((v, par))
    escolhidos = {par for lista in ranking.values() for _, par in sorted(lista, reverse=True)[:TOPO]}
    return sorted(escolhidos), {"status": "ok", "candidatos": len(candidatos),
                                "selecionados": len(escolhidos),
                                "falhas": sum(meses is None for _, meses in tabela)}


def main():
    agora = datetime.now(timezone.utc)
    fim = ms(agora.year, agora.month)
    with open("pares.txt", encoding="utf-8") as arquivo:
        manuais = sorted({p.strip().upper() for p in arquivo if p.strip() and not p.startswith("#")})
    try:
        automaticos, selecao = universo(fim)
    except Exception as falha:
        automaticos, selecao = [], {"status": "erro: " + str(falha)}

    def um(par):
        try:
            situacao = atualizar(par, fim)
        except Exception as falha:                     # um par com problema nao derruba os outros
            situacao = {"status": "erro: " + str(falha)}
        print(par, situacao, flush=True)
        return par, situacao

    with ThreadPoolExecutor(FILAS) as fila:
        pares = dict(fila.map(um, sorted(set(manuais) | set(automaticos))))
    manifesto = {"atualizado_em": agora.strftime("%Y-%m-%dT%H:%MZ"), "dados_ate": iso(fim - HORA),
                 "fonte": "Binance, dados publicos de mercado (velas de 1h)",
                 "selecao_automatica": selecao, "pares_manuais": manuais, "pares": pares}
    with open("manifesto.json", "w", encoding="utf-8") as arquivo:
        json.dump(manifesto, arquivo, indent=1)


if __name__ == "__main__":
    main()
