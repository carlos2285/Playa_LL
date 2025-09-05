# Dashboard Territorio (Streamlit) — Plan de Tabulados

## Ejecutar localmente
pip install -r requirements.txt
streamlit run app.py

## Datos por defecto
- data/estructura_hogar_etiquetada.xlsx
- data/metadata/Codebook.xlsx

## Tips para Streamlit Cloud
- Sube todo el folder `data/` al repo/app (mantén los mismos nombres y rutas).
- Si cambias nombres o rutas, ajusta en la barra lateral.
- Revisa que `p004` exista para los filtros por vivienda/negocio/mixto.
