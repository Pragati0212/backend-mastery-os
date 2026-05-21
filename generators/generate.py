import os
import sys
from google import genai
from dotenv import load_dotenv

load_dotenv()

CONTENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'content'))
os.makedirs(CONTENT_DIR, exist_ok=True)

def generate_topic(topic_name):
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key or api_key == "your_gemini_api_key_here":
        print("❌ Error: Valid GEMINI_API_KEY missing in .env")
        sys.exit(1)
        
    client = genai.Client(api_key=api_key)
    
    prompt = f"""You are a world-class Staff Backend Engineer and Architect.
    Write a comprehensive, deep-dive technical guide on: '{topic_name}'.
    
    Format the output in pure Markdown. 
    It MUST include:
    1. **Core Concepts:** Clear, low-BS explanations of how it works.
    2. **Architecture Visualized:** At least ONE Mermaid.js diagram illustrating the architecture, request lifecycle, or data flow. Wrap the mermaid code in standard markdown code blocks (```mermaid).
    3. **Production Code Examples:** Realistic, scalable code snippets (avoid toy examples).
    4. **Failure Modes & Scaling:** What goes wrong in production and how to fix it.
    
    Do not include introductory pleasantries. Just output the technical markdown.
    """
    
    print(f"🧠 Asking Gemini 2.5 Pro to engineer: {topic_name}...")
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-pro',
            contents=prompt
        )
        filename = topic_name.lower().replace(' ', '-') + '.md'
        filepath = os.path.join(CONTENT_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"✅ Successfully generated and saved to: {filepath}")
    except Exception as e:
        print(f"❌ Generation Execution Exception: {e}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python generators/generate.py "Topic Name"')
    else:
        generate_topic(sys.argv[1])