import streamlit as st
from config.translations import _


def render_legal():
    st.title(_("nav_legal"))
    tab1, tab2, tab3 = st.tabs(["📋 Mentions Legales", "🔒 RGPD", "📄 CGU"])

    with tab1:
        st.markdown(
            '<div class="legal-text"><h2>Editeur</h2>'
            '<p><strong>Logiflo B2B Enterprise</strong> — SASU (en cours d immatriculation)<br>'
            '16 rue Vacon, 13001 Marseille, France<br>contact@logiflo.io</p>'
            '<h2>Hebergement</h2>'
            '<p>Application : Render (Frankfurt, EU)<br>'
            'Site vitrine : GitHub Pages<br>'
            'Base de donnees : Supabase (Paris, AWS eu-west-3)</p>'
            '<h2>Propriete Intellectuelle</h2>'
            '<p>Tous les elements de LOGIFLO.IO sont la propriete exclusive de Logiflo B2B Enterprise.</p>'
            '<p style="color:#4A6080;font-size:13px;"><em>Version 3.0 — Mai 2026</em></p></div>',
            unsafe_allow_html=True)

    with tab2:
        st.markdown(
            '<div class="legal-text"><h2>Conformite RGPD</h2>'
            '<p>DPO : contact@logiflo.io</p>'
            '<h2>Ce que nous stockons</h2>'
            '<p>Fichiers traites en RAM uniquement. Historique dans Supabase : KPIs, resume IA, PDF.<br>'
            'Chiffrement TLS + AES-256. Suppression sous 30j sur demande.</p>'
            '<h2>Sous-traitants</h2>'
            '<p>OpenAI (USA, DPA RGPD signe) — analyse IA<br>'
            'Google Gemini (fallback) — analyse IA<br>'
            'Supabase (Paris, UE) — stockage resultats</p>'
            '<h2>Vos droits</h2>'
            '<p>Acces, rectification, suppression, portabilite.<br>'
            'contact@logiflo.io — www.cnil.fr</p></div>',
            unsafe_allow_html=True)

    with tab3:
        st.markdown(
            '<div class="legal-text"><h2>Offres</h2>'
            '<p><strong>Gratuit</strong> 1 audit par email<br>'
            '<strong>Pro</strong> 590 EUR/mois (engagement 12 mois) ou 790 EUR audit ponctuel<br>'
            '<strong>Expert</strong> Bientot disponible</p>'
            '<h2>Engagement</h2>'
            '<p>Analyses fournies a titre d aide a la decision uniquement.</p>'
            '<h2>Droit applicable</h2>'
            '<p>Droit francais — Tribunal de commerce de Marseille.</p></div>',
            unsafe_allow_html=True)
