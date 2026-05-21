import os
from flask import Flask, render_template
import markdown

app = Flask(__name__)
CONTENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'content'))

def get_topics():
    if not os.path.exists(CONTENT_DIR):
        return []
    files = [f for f in os.listdir(CONTENT_DIR) if f.endswith('.md')]
    return sorted([f.replace('.md', '') for f in files])

@app.route('/')
def index():
    return render_template('index.html', topics=get_topics())

@app.route('/topic/<name>')
def topic(name):
    filepath = os.path.join(CONTENT_DIR, f"{name}.md")
    if not os.path.exists(filepath):
        return render_template('topic.html', title="Not Found", content="<h2>Topic not found.</h2>", topics=get_topics()), 404
    
    with open(filepath, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    html_content = markdown.markdown(md_content, extensions=['fenced_code', 'tables'])
    return render_template('topic.html', title=name.replace('-', ' ').title(), content=html_content, topics=get_topics())

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)