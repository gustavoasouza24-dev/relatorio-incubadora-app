import streamlit as st
import pandas as pd
from datetime import timedelta

st.set_page_config(page_title="Relatório Incubadora", page_icon="📄", layout="wide")

DEFAULT_GAP_DIAS = 3
DEFAULT_TERMOS_ATIVO = "andamento|ativo"

def read_csv_uploaded(uploaded_file) -> pd.DataFrame:
    last_err = None
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            uploaded_file.seek(0)
            return pd.read_csv(
                uploaded_file, sep=None, engine="python", encoding=enc, on_bad_lines="skip"
            )
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Falha ao ler o CSV. Último erro: {last_err}")

def clean_num(val):
    if pd.isna(val): return 0.0
    if isinstance(val, str):
        v = (val.replace('"', "").replace("'", "").replace("R$", "")
                .replace(" ", "").strip())
        if "," in v and "." in v: v = v.replace(".", "").replace(",", ".")
        elif "," in v: v = v.replace(",", ".")
    else:
        v = val
    try: return float(v)
    except: return 0.0

def detectar_colunas(df: pd.DataFrame) -> dict:
    col_map = {"motorista":None,"dias":None,"inicio":None,"fim":None,"status":None,"valor":None,"contrato":None,"veiculo":None}
    for c in df.columns:
        cl = c.lower()
        if "motorista" in cl: col_map["motorista"] = c
        elif "dias" in cl: col_map["dias"] = c
        elif "início" in cl or "inicio" in cl or "início do" in cl: col_map["inicio"] = c
        elif "fim" in cl or "término" in cl or "termino" in cl: col_map["fim"] = c
        elif "status" in cl: col_map["status"] = c
        elif ("total" in cl and "mot" in cl) or ("valor" in cl and "mot" in cl) or cl == "valor": col_map["valor"] = c
        elif cl == "contrato" or ("contrato" in cl and "status" not in cl and "total" not in cl): col_map["contrato"] = c
        elif "tipo" in cl and ("ve" in cl or "veículo" in cl or "veiculo" in cl): col_map["veiculo"] = c
    essenciais = ["motorista","dias","inicio","fim","status","valor"]
    faltantes = [k for k in essenciais if not col_map[k]]
    if faltantes:
        raise ValueError(
            "Não encontrei as colunas essenciais: " + ", ".join(f"'{f}'" for f in faltantes)
            + ". Verifique os nomes no CSV."
        )
    return col_map

def preparar_alvos(df_targets: pd.DataFrame) -> list[str]:
    cols = [c.lower() for c in df_targets.columns]
    if "nome" in cols:
        col = df_targets.columns[cols.index("nome")]
    elif "motorista" in cols:
        col = df_targets.columns[cols.index("motorista")]
    else:
        col = df_targets.columns[0]
    nomes_norm = (
        df_targets[col].astype(str).str.upper().str.strip().dropna().drop_duplicates().tolist()
    )
    return nomes_norm

def analisar_contratos(grupo: pd.DataFrame, col_map: dict, gap_max_dias: int, termos_ativo_regex: str):
    grupo = grupo.sort_values("dt_inicio").reset_index(drop=True)
    linhas, ultimo_fim = [], None
    status_geral = "Formado"
    for _, row in grupo.iterrows():
        tipo_contrato, is_renovacao = "Novo", False
        if pd.notna(row["dt_inicio"]) and pd.notna(ultimo_fim):
            if row["dt_inicio"] - ultimo_fim <= timedelta(days=gap_max_dias):
                is_renovacao, tipo_contrato = True, "Renovação"
        status_row = str(row[col_map["status"]]) if col_map["status"] else ""
        ativo = any(t.strip().lower() in status_row.lower() for t in termos_ativo_regex.split("|") if t.strip())
        contrato_id = row[col_map["contrato"]] if col_map["contrato"] else "N/D"
        veiculo_tipo = row[col_map["veiculo"]] if col_map["veiculo"] else "N/D"
        linhas.append({
            "Contrato": contrato_id, "Tipo Veículo": veiculo_tipo,
            "Início": row["dt_inicio"], "Fim": row["dt_fim"],
            "Dias": row["dias_clean"], "Valor": row["valor_clean"],
            "Status Base": status_row, "Tipo Contrato": tipo_contrato
        })
        if ativo:
            status_geral = "Em formação (Renovado)" if is_renovacao else "Em formação"
        if pd.notna(row["dt_fim"]):
            ultimo_fim = row["dt_fim"]
    df_prest = pd.DataFrame(linhas)
    if not df_prest.empty:
        df_prest.insert(0, "Status Atual do Motorista", status_geral)
    return status_geral, df_prest

def processar(df_contratos: pd.DataFrame, df_targets: pd.DataFrame, gap_max_dias: int, termos_ativo_regex: str, formato_br: bool):
    col_map = detectar_colunas(df_contratos)
    df_contratos["motorista_norm"] = df_contratos[col_map["motorista"]].astype(str).str.upper().str.strip()
    alvos = preparar_alvos(df_targets)
    df_filt = df_contratos[df_contratos["motorista_norm"].isin(alvos)].copy()
    if df_filt.empty: return pd.DataFrame()
    df_filt["dt_inicio"] = pd.to_datetime(df_filt[col_map["inicio"]], dayfirst=True, errors="coerce")
    df_filt["dt_fim"] = pd.to_datetime(df_filt[col_map["fim"]], dayfirst=True, errors="coerce")
    df_filt["dias_clean"] = df_filt[col_map["dias"]].apply(clean_num).round(0).astype("Int64")
    df_filt["valor_clean"] = df_filt[col_map["valor"]].apply(clean_num)
    lista = []
    for motorista, grupo in df_filt.groupby("motorista_norm", sort=False):
        _, df_prest = analisar_contratos(grupo, col_map, gap_max_dias, termos_ativo_regex)
        if not df_prest.empty:
            df_prest.insert(0, "Motorista", motorista)
            lista.append(df_prest)
    if not lista: return pd.DataFrame()
    out = pd.concat(lista, ignore_index=True)
    out["Início"] = pd.to_datetime(out["Início"]).dt.strftime("%d/%m/%Y")
    out["Fim"] = pd.to_datetime(out["Fim"]).dt.strftime("%d/%m/%Y")
    out["Valor"] = out["Valor"].apply(
        (lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")) if formato_br else (lambda x: f"{x:.2f}")
    )
    return out

st.title("📄 Relatório Incubadora — Upload de CSVs")
st.markdown("""
Envie **2 arquivos CSV**:

1. **Base de contratos** (com as colunas *motorista, dias, início, fim, status, valor* e, opcionalmente, *contrato*, *tipo de veículo*).
2. **Prestadores‑alvo** (uma coluna com os nomes; pode ser `Nome`, `Motorista` ou a **primeira coluna**).
""")

col_l, col_r = st.columns(2)
with col_l:
    base_file = st.file_uploader("📥 Base de contratos (.csv)", type=["csv"], key="base_csv")
with col_r:
    targets_file = st.file_uploader("🎯 Prestadores‑alvo (.csv)", type=["csv"], key="targets_csv")

with st.expander("⚙️ Parâmetros (opcional)", expanded=False):
    gap_max = st.number_input("Janela de renovação (dias) — 'Renovação' se gap ≤", min_value=0, max_value=30, value=DEFAULT_GAP_DIAS, step=1)
    termos_ativo = st.text_input("Termos que indicam status 'Ativo' (separados por '|')", value=DEFAULT_TERMOS_ATIVO)
    formato_br = st.toggle("Formatar valores em R$ (padrão BR)", value=True)

if st.button("🚀 Processar", type="primary", use_container_width=True):
    if not base_file or not targets_file:
        st.warning("Envie os dois arquivos CSV para continuar.")
        st.stop()
    try:
        df_base = read_csv_uploaded(base_file)
        df_targets = read_csv_uploaded(targets_file)
        resultado = processar(df_base, df_targets, gap_max, termos_ativo, formato_br)
    except Exception as e:
        st.error(f"Erro no processamento: {e}")
        st.stop()

    if resultado is None or resultado.empty:
        st.info("Nenhuma linha para exibir. Verifique se os nomes dos prestadores no arquivo-alvo existem na base de contratos.")
    else:
        st.success(f"Processamento concluído: {len(resultado):,} linhas.", icon="✅")
        st.dataframe(resultado, use_container_width=True)

        csv_bytes = resultado.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "💾 Baixar CSV final",
            data=csv_bytes,
            file_name="Relatorio_Incubadora_Final.csv",
            mime="text/csv",
            use_container_width=True,
        )

