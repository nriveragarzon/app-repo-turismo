import streamlit as st
import os

def main():
    # Bloque de CSS personalizado
    st.markdown(
        """
        <style>
        /* Clase para el mensaje de mantenimiento */
        .maintenance-text {
            font-size: 2rem; /* Ajusta el tamaño del texto */
            font-weight: bold; /* Hazlo un poco más grueso */
            color: #FF0000; /* Color principal (rojo) */
            text-align: center; 
            margin-top: 2rem; 
            
            /* Animación */
            animation: vibrate 1.5s infinite;
        }

        /* Definimos la animación 'vibrate' */
        @keyframes vibrate {
            0%   { transform: translate(0, 0); }
            20%  { transform: translate(-2px, 2px); }
            40%  { transform: translate(2px, -2px); }
            60%  { transform: translate(-2px, 2px); }
            80%  { transform: translate(2px, -2px); }
            100% { transform: translate(0, 0); }
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Mensaje de mantenimiento
    st.markdown(
        '<div class="maintenance-text">'
        'La aplicación está en mantenimiento.<br>'
        'La Gerencia de Inteligencia Comercial está actualizando tu aplicación.<br>'
        'Por favor, vuelve más tarde.'
        '</div>',
        unsafe_allow_html=True
    )

    # Ruta del GIF descargado 
    gif_path = "static/images/mantenimiento.gif"

    # Verificamos si el archivo existe antes de mostrarlo
    if os.path.exists(gif_path):
        st.image(gif_path, use_container_width=True)
    else:
        st.write("Trabajando.....")

if __name__ == '__main__':
    main()
