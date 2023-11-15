import streamlit as st

def homePage():
    st.header(':red[Data Qulity Engine]')
    st.markdown("**----------------------------------------------------------------------------------------------------------------------------------------**")
    st.markdown('A Data Quality Engine For Data Engineers By Data Engineers.')
    st.markdown("**----------------------------------------------------------------------------------------------------------------------------------------**")

    if st.button("Get Started"):

        from DQEngine.dqengine import dqEngine
        dqEngine()


