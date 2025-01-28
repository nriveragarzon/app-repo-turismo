# Librerías
from docx import Document
from docx.shared import Pt, RGBColor, Cm, Inches
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH

def customize_style(style, font_name, font_size, font_color, bold=False, underline=False, alignment=WD_PARAGRAPH_ALIGNMENT.LEFT):
    """
    Personaliza un estilo de texto en un documento de python-docx.

    Args:
    style: Estilo del documento a personalizar (e.g., 'Title', 'Heading 1').
    font_name (str): Nombre de la fuente.
    font_size (Pt): Tamaño de la fuente.
    font_color (RGBColor): Color de la fuente (RGB).
    bold (bool): Indica si el texto será negrita. Por defecto es False.
    underline (bool): Indica si el texto estará subrayado. Por defecto es False.
    alignment (WD_PARAGRAPH_ALIGNMENT): Alineación del texto (e.g., LEFT, CENTER, RIGHT, JUSTIFY).
    """
    style.font.name = font_name
    style.font.size = font_size
    style.font.color.rgb = font_color
    style.font.bold = bold
    style.font.underline = underline
    if hasattr(style, 'paragraph_format'):  # Verificar si tiene formato de párrafo
        style.paragraph_format.alignment = alignment

def estilos(doc: Document):
    """
    Define y aplica estilos personalizados a un documento de python-docx.

    Args:
    doc (Document): El documento al que se añadirán los estilos personalizados.
    """

    # Personalizar el estilo 'Title'
    title_style = doc.styles['Title']
    customize_style(title_style, 'Century Gothic', Pt(16), RGBColor(0, 32, 96), bold=True, alignment=WD_PARAGRAPH_ALIGNMENT.CENTER)

    # Personalizar el estilo 'Heading 1'
    heading1_style = doc.styles['Heading 1']
    customize_style(heading1_style, 'Century Gothic', Pt(14), RGBColor(0, 32, 96), bold=True, alignment=WD_PARAGRAPH_ALIGNMENT.LEFT)

    # Personalizar el estilo 'Heading 2'
    heading2_style = doc.styles['Heading 2']
    customize_style(heading2_style, 'Century Gothic', Pt(12), RGBColor(0, 32, 96), bold=True, alignment=WD_PARAGRAPH_ALIGNMENT.LEFT)

    # Personalizar el estilo 'Heading 3'
    heading3_style = doc.styles['Heading 3']
    customize_style(heading3_style, 'Century Gothic', Pt(11), RGBColor(0, 32, 96), bold=True, alignment=WD_PARAGRAPH_ALIGNMENT.LEFT)

    # Personalizar el estilo 'Normal'
    normal_style = doc.styles['Normal']
    customize_style(normal_style, 'Century Gothic', Pt(11), RGBColor(0, 0, 0), alignment=WD_PARAGRAPH_ALIGNMENT.JUSTIFY)

    # Personalizar el estilo 'Table Grid' (Tablas)
    table_style = doc.styles['Table Grid']
    customize_style(table_style, 'Century Gothic', Pt(10), RGBColor(0, 0, 0), alignment=WD_PARAGRAPH_ALIGNMENT.CENTER)

    # Crear un estilo personalizado para captions (leyendas o notas)
#    if 'Caption' not in doc.styles:
    caption_style = doc.styles.add_style('Custom Caption', 1)  # Tipo 1: Estilo de párrafo
 #   else:
    #    caption_style = doc.styles['Custom Caption']
    customize_style(caption_style, 'Century Gothic', Pt(9), RGBColor(128, 128, 128), alignment=WD_PARAGRAPH_ALIGNMENT.LEFT)

    # Ajustar las márgenes del documento 
    # Márgenes Papeleria Modelo Procolombia
    for section in doc.sections:
        section.top_margin = Cm(2.57)  # Margen superior
        section.bottom_margin = Cm(0.51)  # Margen inferior
        section.left_margin = Cm(2.54)  # Margen izquierdo
        section.right_margin = Cm(2.54)  # Margen derecho

    # Personalizar estilos adicionales
    additional_styles = ['Quote', 'Intense Quote', 'List Bullet', 'List Number', 'Subtitle']
    for style_name in additional_styles:
        if style_name in doc.styles:
            style = doc.styles[style_name]
            customize_style(style, 'Century Gothic', Pt(11), RGBColor(0, 0, 0), alignment=WD_PARAGRAPH_ALIGNMENT.LEFT)

    # Personalizar estilos de tabla genéricos
    for table_style_name in ['Table Grid', 'Light Shading', 'Light List', 'Medium Shading 1']:
        if table_style_name in doc.styles:
            table_style = doc.styles[table_style_name]
            customize_style(table_style, 'Century Gothic', Pt(10), RGBColor(0, 0, 0), alignment=WD_PARAGRAPH_ALIGNMENT.CENTER)

