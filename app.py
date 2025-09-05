import streamlit as st
import pandas as pd
import numpy as np
import joblib
import os
import tensorflow as tf
from datetime import datetime, date, time, timedelta
import pytz
from templates.config import LOCATION_DICT, WEATHER_DICT, NY_TZ, HIST_PATH, REQUIRED_TARGET_DERIVED
from langchain_groq import ChatGroq
from dotenv import load_dotenv
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
import io

# Load environment
load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

@st.cache_resource(show_spinner=False)
def load_assets():
    model = tf.keras.models.load_model('final_models/model.keras')
    preprocessor = joblib.load('final_models/preprocessor.pkl')
    llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=0.5, groq_api_key=GROQ_API_KEY)

    explain_prompt_template = PromptTemplate.from_template(
        "Given the following prediction context:\n"
        "Location: {location_name}\n"
        "Date & Hour: {date_input} {hour_input}:00\n"
        "Weather: {weather_name}, Rain: {is_rain}, Holiday: {is_holiday}\n"
        "Temp: {temperature}°C, Precip: {precipitation}mm\n"
        "Predicted rides: {pred_val}\n"
        "Explain the main factors influencing this prediction in a detailed, user-friendly way suitable for non-technical audiences. "
        "Structure the response with markdown headings in '###'. "
        "Ensure each section uses a numbered list to list the factors."
        "Use appropriate emojis for headings to make it look professional and interactive."
    )

    report_prompt_template = PromptTemplate.from_template(
        "Generate a professional analysis report for operations teams on taxi demand prediction. "
        "Summarize demand factors, recent predictions and their context, and provide actionable insights in markdown."
        "\nContext: {input_df_dict}, Predicted rides: {pred_val}."
        "Structure the response with markdown headings in '###', and not '**'. "
    )

    scenario_prompt_template = PromptTemplate.from_template(
        "Current prediction context:\n"
        "Location: {location_name}, Date: {date_input}, Hour: {hour_input}, Weather: {weather_name}, "
        "Holiday: {is_holiday}, Rain: {is_rain}, Temp: {temperature}, Precip: {precipitation}, Rides: {pred_val}.\n"
        "User scenario: {scenario_query}\n"
        "Please provide a detailed but easy-to-understand explanation of how demand would change compared to the current prediction, suitable for non-technical users. "
        "Structure the response with markdown headings in '###'."
        "Use appropriate emojis for headings to make it look professional and interactive."
    )

    explanation_chain = LLMChain(llm=llm, prompt=explain_prompt_template)
    report_chain = LLMChain(llm=llm, prompt=report_prompt_template)
    scenario_chain = LLMChain(llm=llm, prompt=scenario_prompt_template)

    qa_prompt_template = PromptTemplate.from_template(
    "You are a helpful assistant for a taxi demand forecasting application. "
    "Answer the user's question clearly and concisely. "
    "If the user asks for a general explanation of a term or feature, provide a brief explanation, possibly referencing the provided project document or historical data summary as an example. "
    "If the user's question can only be answered from the data, base your answer on the provided summary. "
    "If the information is not available in the document or data, respond politely that the answer is not available. \n\n"
    "Project Document:\n{project_doc}\n\n"
    "Historical Data Summary:\n{historical_data}\n\n"
    "User's Question:\n{query}"
    )

    qa_chain = LLMChain(llm=llm, prompt=qa_prompt_template)

    return model, preprocessor, llm, explanation_chain, report_chain, scenario_chain, qa_chain

@st.cache_data(show_spinner=False)
def load_history():
    df = pd.read_csv(HIST_PATH)
    df['pickup_hour'] = pd.to_datetime(df['pickup_hour'], errors='coerce', utc=True)
    df = df.dropna(subset=['pickup_hour'])
    df['pickup_hour'] = df['pickup_hour'].dt.tz_convert(NY_TZ)
    df['ride_count'] = df['ride_count'].fillna(0)
    return df.sort_values(['PULocationID', 'pickup_hour']).reset_index(drop=True)

def _ensure_ts(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(NY_TZ)
    else:
        ts = ts.tz_convert(NY_TZ)
    return ts.replace(minute=0, second=0, microsecond=0)

def build_target_derived_features(history, loc_id, target_ts):
    g = history[history['PULocationID'] == loc_id][['pickup_hour', 'ride_count']]
    new_row = pd.DataFrame({'pickup_hour': [_ensure_ts(target_ts)], 'ride_count': [np.nan]})
    g2 = pd.concat([g, new_row], ignore_index=True).sort_values('pickup_hour').reset_index(drop=True)
    g2['ride_count_lag_1'] = g2['ride_count'].shift(1)
    g2['ride_count_lag_24'] = g2['ride_count'].shift(24)
    g2['ride_count_lag_168'] = g2['ride_count'].shift(168)
    shifted = g2['ride_count'].shift(1)
    g2['ride_count_roll_mean_3'] = shifted.rolling(3).mean()
    g2['ride_count_roll_std_3'] = shifted.rolling(3).std()
    row = g2[g2['pickup_hour'] == _ensure_ts(target_ts)].tail(1)
    feats = row[REQUIRED_TARGET_DERIVED].to_dict(orient='records')
    if not feats:
        feats = [{k: np.nan for k in REQUIRED_TARGET_DERIVED}]
    return {k: 0.0 if pd.isna(v) else float(v) for k, v in feats[0].items()}

def assemble_input_row(loc_id, target_ts, weather_name, is_holiday, is_rain, temperature, precipitation):
    ts = _ensure_ts(target_ts)
    weather_code = next((code for code, name in WEATHER_DICT.items() if name == weather_name), 0)
    day_of_week = ts.weekday()
    month = ts.month
    hour = ts.hour
    is_weekend = int(day_of_week >= 5)
    return pd.DataFrame([{
        'PULocationID': loc_id,
        'weathercode': weather_code,
        'hour': hour,
        'day_of_week': day_of_week,
        'month': month,
        'is_weekend': is_weekend,
        'is_holiday': int(is_holiday),
        'is_rain': int(is_rain),
        'temperature_2m': float(temperature),
        'precipitation': float(precipitation),
    }])

def display_structured_text(md_text):
    st.markdown(md_text)

def clear_prediction():
    st.session_state['pred_val'] = None
    st.session_state['prediction_context'] = None
    st.session_state['scenario_query'] = ""
    st.session_state['scenario_response'] = ""
    st.session_state['explanation_text'] = ""

def generate_pdf_report(report_md):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()

    # Split markdown into paragraphs, recognizing headers
    lines = report_md.split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('###'):
            story.append(Paragraph(line.replace('###', '<b>', 1) + '</b>', styles['Heading2']))
        elif line.startswith('##'):
            story.append(Paragraph(line.replace('##', '<b>', 1) + '</b>', styles['Heading1']))
        elif line:
            story.append(Paragraph(line, styles['Normal']))
            story.append(Spacer(1, 12))

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()

## Initialize session state keys
if 'pred_val' not in st.session_state:
    clear_prediction()

st.set_page_config(page_title="Taxi Demand Forecasting", page_icon="🚕", layout="centered")
st.title("🚕 Taxi Demand Forecasting")

model, preprocessor, llm, explanation_chain, report_chain, scenario_chain, qa_chain = load_assets()
hist_df = load_history()

with st.sidebar:
    st.header("Inputs (edit or review)")
    location_name = st.selectbox(
        "Pickup Location", options=[None] + list(LOCATION_DICT.keys()), index=0,
        format_func=lambda x: "Select Location" if x is None else x,
        key="location_select"
    )
    weather_name = st.selectbox(
        "Weather Condition", options=[None] + list(WEATHER_DICT.values()), index=0,
        format_func=lambda x: "Select Weather" if x is None else x,
        key="weather_select"
    )
    default_dt = (hist_df['pickup_hour'].max().astimezone(NY_TZ) + timedelta(hours=1)).date()
    date_input = st.date_input("Prediction Date (NYC)", value=default_dt, key="date_input")
    hour_input = st.slider(
        "Hour of Day (0-23)", 0, 23,
        value=(hist_df['pickup_hour'].max().hour + 1) % 24,
        key="hour_input"
    )
    is_holiday = st.checkbox("Is Holiday?", value=False, key="holiday_check")
    is_rain = st.checkbox("Is it raining?", value=False, key="rain_check")
    temperature_2m = st.slider(
        "Temperature (°C)", -20.0, 40.0,
        15.0, help="Current air temperature", key="temp_slider"
    )
    precipitation = st.slider(
        "Precipitation (mm)", 0.0, 50.0, 0.0, help="Precipitation this hour", key="precip_slider"
    )
    if st.button("Clear Prediction & Scenarios", help="Clear all previous prediction results and what-if scenarios."):
        clear_prediction()
        st.rerun()

    st.markdown("----")
    st.header("📖 Ask about the app, project, inputs or predictions!")
    bot_query = st.text_input("Ask the assistant \n( e.g. What is precipitation (mm)? \n or \n Explain how precipitation (mm) = 21.21 will affect the prediction?)", key="chatbot_query", label_visibility="visible")
    if bot_query:
        historical_summary = hist_df.head(50).to_markdown(index=False, numalign="left", stralign="left")
        project_doc_summary = "This project predicts NYC taxi demand using historical data, weather, and holidays. It uses TensorFlow models, preprocessing, and Streamlit dashboard for interactive predictions."
        response = qa_chain.invoke({
            "query": bot_query,
            "historical_data": historical_summary,
            "project_doc": project_doc_summary
        })
        answer = response.get("text")
        display_structured_text(answer)

# Main content container
main_content = st.empty()
with main_content.container():
    if location_name is None or weather_name is None:
        st.warning("Please select both location and weather to enable prediction.")
        st.stop()

    pred_ts = _ensure_ts(datetime.combine(date_input, time(hour_input)))
    loc_id = LOCATION_DICT.get(location_name)
    if loc_id is None:
        st.error("Invalid location selected.")
        st.stop()
    if pred_ts <= hist_df['pickup_hour'].min():
        st.error(f"Prediction timestamp {pred_ts} must be later than historical start {hist_df['pickup_hour'].min()}.")
        st.stop()

    input_df = assemble_input_row(loc_id, pred_ts, weather_name, is_holiday, is_rain, temperature_2m, precipitation)
    
    if st.button("Predict 🚕", key="predict_button"):
        clear_prediction()
        
        lag_feats = build_target_derived_features(hist_df, loc_id, pred_ts)
        for k, v in lag_feats.items():
            input_df[k] = v
        
        try:
            transformed_input = preprocessor.transform(input_df)
        except Exception as e:
            st.error(f"Preprocessing error: {e}")
            st.stop()
        
        input_list = []
        start_idx = 0
        for shape in model.inputs:
            cols = shape.shape[1]
            input_list.append(transformed_input[:, start_idx: start_idx + cols])
            start_idx += cols
        
        with st.spinner("Predicting demand..."):
            pred = model.predict(input_list)
        
        pred_val = int(np.round(np.clip(pred[0], 0, None)))
        st.session_state['pred_val'] = pred_val
        st.session_state['prediction_context'] = {
            "location_name": location_name,
            "date_input": str(date_input),
            "hour_input": hour_input,
            "weather_name": weather_name,
            "is_rain": is_rain,
            "is_holiday": is_holiday,
            "temperature": temperature_2m,
            "precipitation": precipitation
        }
        
        explanation_resp = explanation_chain.invoke({**st.session_state['prediction_context'], 'pred_val': pred_val})
        st.session_state['explanation_text'] = explanation_resp.get("text", "")
        
        st.rerun()

    # Display prediction results only if they exist in session state
    if st.session_state['pred_val'] is not None:
        st.subheader("Predicted Taxi Demand 📈 ")
        st.metric(label="Estimated number of rides", value=f"{st.session_state['pred_val']:,}")
        
        with st.expander("Why this prediction? 🔎"):
            st.markdown(st.session_state['explanation_text'])
            
        with st.expander("Show input features 📋"):
            st.dataframe(input_df)
            
        st.markdown("---")
        st.subheader("What-If Scenario Analysis 🔮💡")
        scenario_input = st.text_input("Describe a what-if scenario (e.g. What if it snowed at 9 P.M. that day?)", key="scenario_input_main")

        if st.button("Run Scenario", key="run_scenario_button"):
            if scenario_input:
                with st.spinner("Running what-if scenario..."):
                    scenario_params = st.session_state['prediction_context'].copy()
                    scenario_params['pred_val'] = st.session_state['pred_val']
                    scenario_params['scenario_query'] = scenario_input
                    scenario_resp = scenario_chain.invoke(scenario_params)
                    st.session_state['scenario_response'] = scenario_resp.get('text', '')
                    st.session_state['scenario_query_display'] = scenario_input
                st.rerun()
            else:
                st.warning("Please enter a scenario query.")

        if 'scenario_response' in st.session_state and st.session_state['scenario_response']:
            st.markdown("### Scenario Result 💡")
            st.markdown(f"**Question:** {st.session_state['scenario_query_display']}")
            st.markdown(st.session_state['scenario_response'])

            
        st.markdown("---")
        
        # Report Export Section
        st.subheader("Export Analysis Report 📄")
        report_resp = report_chain.invoke({
            "input_df_dict": input_df.to_dict(),
            "pred_val": st.session_state['pred_val']
        })
        report_md = report_resp.get("text", "")

        st.download_button(
            "Download Report as PDF",
            data=generate_pdf_report(report_md),
            file_name="Taxi_Demand_Report.pdf",
            mime="application/pdf",
        )