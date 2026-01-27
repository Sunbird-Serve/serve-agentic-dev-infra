@echo off
echo Installing Streamlit dependencies...
pip install -r requirements-streamlit.txt

echo.
echo Starting Streamlit app...
streamlit run streamlit_app.py

