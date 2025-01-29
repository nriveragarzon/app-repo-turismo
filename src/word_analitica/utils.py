# Librerías
from docx import Document
from docx.shared import Pt, Inches, Cm
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

def add_header_footer(doc: Document, header_image_left: str, footer_image: str, footer_text: str):
    """
    Agrega un encabezado y un pie de página a la primera sección de un documento de Word.
    
    En el encabezado se inserta una tabla con una sola fila y dos columnas:
    - Imagen en la celda izquierda con dimensiones personalizadas (7.66 cm de ancho, 3.4 cm de alto).
    - La celda derecha se mantiene vacía (podrías ampliarla según tus necesidades).

    En el pie de página se inserta una tabla con una sola fila y tres columnas:
    - Texto en la celda izquierda (alineado a la izquierda).
    - Número de página en la celda central (alineado al centro).
    - Imagen en la celda derecha (alineada a la derecha), con dimensiones personalizadas (5.99 cm de ancho, 1.27 cm de alto).

    Args:
        doc (Document): El objeto Document al que se añadirán encabezado y pie de página.
        header_image_left (str): Ruta de la imagen que se ubicará en la parte izquierda del encabezado.
        footer_image (str): Ruta de la imagen que se colocará en la parte derecha del pie de página.
        footer_text (str): Texto que aparecerá en la parte izquierda del pie de página.
    """
    # Obtener la primera sección del documento
    section = doc.sections[0]
    # Ajustar la distancia del encabezado y el pie de página
    section.header_distance = Cm(2.57)
    section.footer_distance = Cm(0.51)

 # Encabezado
    header = section.header
    #header_table = header.add_table(rows=1, cols=2, width=doc.sections[0].page_width)
    #header_table.autofit = True

    # Imagen izquierda en el encabezado
    #header_cell_left = header_table.cell(0, 0)
    header_paragraph_left = header.paragraphs[0]
    header_run_left = header_paragraph_left.add_run()
    header_run_left.add_picture(header_image_left, width=Inches(2.5))

    # Pie de página
    footer = section.footer
    footer_table = footer.add_table(rows=1, cols=3, width=doc.sections[0].page_width)
    footer_table.autofit = True

    # Texto izquierda en el pie de página
    footer_cell_left = footer_table.cell(0, 0)
    footer_paragraph_left = footer_cell_left.paragraphs[0]
    footer_paragraph_left.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
    footer_run_left = footer_paragraph_left.add_run("Calle 30 # 13ª - 15, Edificio CCI Pisos 35 - 36 | Bogotá, Colombia T: +57 (1) 560 0100 | info@procolombia.co | www.procolombia.co")
    footer_run_left.font.size = Pt(8)

    # Número de página en el centro del pie de página
    footer_cell_center = footer_table.cell(0, 1)
    footer_paragraph_center = footer_cell_center.paragraphs[0]
    footer_paragraph_center.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    footer_run_center = footer_paragraph_center.add_run()
    fldChar1 = OxmlElement('w:fldChar')
    fldChar1.set(qn('w:fldCharType'), 'begin')
    footer_run_center._r.append(fldChar1)
    instrText = OxmlElement('w:instrText')
    instrText.text = 'PAGE'
    footer_run_center._r.append(instrText)
    fldChar2 = OxmlElement('w:fldChar')
    fldChar2.set(qn('w:fldCharType'), 'end')
    footer_run_center._r.append(fldChar2)
    # Establecer el tamaño de la fuente del número de página
    footer_run_center.font.size = Pt(8)

    # Imagen derecha en el pie de página
    footer_cell_right = footer_table.cell(0, 2)
    footer_paragraph_right = footer_cell_right.paragraphs[0]
    footer_paragraph_right.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
    footer_run_right = footer_paragraph_right.add_run()
    footer_run_right.add_picture(footer_image, width=Inches(2.0))