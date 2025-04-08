import gradio as gr

from fourty_one import process_with_41
from ninety import process_with_90
def make_visible():
    return gr.update(visible=True)


def process_41(file_input, file_prefix, skip_lines):
    gr.Info('Процесс пошёл!')
    return process_with_41(file_input, file_prefix, skip_lines)
def process_90(file_input, file_prefix, skip_lines):
    gr.Info('Процесс пошёл!')
    return process_with_90(file_input, file_prefix, skip_lines)

with gr.Blocks() as app:
    file_input = gr.File(label='Выберите txt файлы', file_count='multiple', file_types=['.txt'])
    file_prefix = gr.Textbox(label='Префикс к названию файла (опционально)', value='41')
    with gr.Tab('Форма 41'):
        gr.Markdown('# Форма 41')
        btn = gr.Button('в csv')
        download_csv = gr.DownloadButton(label='Скачать csv', visible=False)
        skip_lines = gr.Number(label='Пропустить в каждом txt файле линий (шапки):', precision=0, value=7, minimum=0.0)
        btn.click(fn=process_41, inputs=[file_input, file_prefix, skip_lines], outputs=[download_csv]).success(lambda: gr.Info("Успех!")).then(fn=make_visible, outputs=[download_csv])
    with gr.Tab('Форма 90'):
        gr.Markdown('# Форма 90')
        btn = gr.Button('в csv')
        download_csv = gr.DownloadButton(label='Скачать csv', visible=False)
        skip_lines = gr.Number(label='Пропустить в каждом txt файле линий (шапки):', precision=0, value=7, minimum=0.0)
        btn.click(fn=process_90, inputs=[file_input, file_prefix, skip_lines], outputs=[download_csv]).success(lambda: gr.Info("Успех!")).then(fn=make_visible, outputs=[download_csv])
        

app.launch(inbrowser=True)
