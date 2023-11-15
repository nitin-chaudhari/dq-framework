import streamlit as st
from module.home import homePage
from module.about import aboutPage
from streamlit_option_menu import option_menu

if __name__ == '__main__':

    hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)
    
    with st.sidebar:
        selected = option_menu("Menu", ["Home",'About'], 
            icons=['house','person-circle'], menu_icon="filter-left", default_index=0)
    
    if selected == 'Home':
        homePage()
    elif selected == 'About':
        aboutPage()
                        
            
        