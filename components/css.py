import streamlit as st

def inject_css():
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');
:root{--navy:#0B2545;--navy2:#162D52;--green:#00C896;--green2:#00A87A;--slate:#4A6080;--light:#F0F4F8;--red:#E8304A;--orange:#f39c12;--white:#FFFFFF;}
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;color:var(--navy);}
.block-container{padding-top:2rem!important;padding-bottom:2rem!important;max-width:95%!important;}
.kpi-card{background:var(--white);padding:24px;border-radius:12px;border:1px solid #e2e8f0;border-top:3px solid var(--green);box-shadow:0 4px 6px -1px rgba(0,0,0,0.05);}
.kpi-card h4{color:var(--slate)!important;font-size:0.75rem!important;text-transform:uppercase;font-weight:600;letter-spacing:1.5px;margin-bottom:10px;}
.kpi-card h2{font-family:'Syne',sans-serif!important;font-size:2.2rem!important;font-weight:800!important;margin-top:0;line-height:1;}
div.stButton>button{border-radius:8px;font-family:'Syne',sans-serif;font-weight:700;background-color:var(--navy);color:#f8fafc;border:none;}
[data-testid="stSidebar"]{background-color:var(--navy)!important;}
[data-testid="stSidebar"] *{color:#ffffff!important;}
.sidebar-logo{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:white;}
.sidebar-logo span{color:#00C896;}
.import-card{background:var(--white);padding:25px;border-radius:12px;border-left:6px solid var(--green);margin-bottom:20px;}
.report-text{background:var(--light);padding:32px;border-radius:12px;border-left:6px solid var(--navy);line-height:1.8;}
.report-text h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:800;color:var(--navy);text-transform:uppercase;letter-spacing:1.5px;margin-top:28px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.report-terrain{background:#f8fff8;padding:28px;border-radius:12px;border-left:6px solid var(--green);line-height:1.9;}
.report-terrain h3{font-family:'Syne',sans-serif;font-size:1rem;font-weight:700;color:var(--green2);margin-top:24px;margin-bottom:8px;}
.archive-card{background:var(--white);border:1px solid #E2EAF4;border-radius:12px;padding:20px;margin-bottom:16px;border-left:4px solid var(--green);}
.legal-text{background:var(--white);padding:32px;border-radius:12px;border:1px solid #E2EAF4;line-height:1.9;}
.legal-text h2{font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:var(--navy);margin-top:28px;padding-bottom:6px;border-bottom:2px solid var(--green);}
.sans-prix-badge{background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:#00A87A;font-size:12px;font-weight:600;padding:4px 12px;border-radius:20px;display:inline-block;margin-bottom:12px;margin-right:8px;}
.mode-badge{display:inline-flex;align-items:center;gap:8px;background:rgba(0,200,150,0.1);border:1px solid rgba(0,200,150,0.3);color:#00A87A;font-size:13px;font-weight:600;padding:8px 16px;border-radius:8px;margin-bottom:16px;}
</style>""", unsafe_allow_html=True)
