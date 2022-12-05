from flask import render_template
import functions_framework

@functions_framework.http
def launch_ui(request):
    return render_template("ui.html")