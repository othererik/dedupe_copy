"""This module contains the web service for visualizing the progress of the dedupe_copy tool."""

import logging
from flask import Flask, jsonify
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models import ColumnDataSource, NumeralTickFormatter
from bokeh.palettes import Category10_10

# Global variable to hold the progress data
progress_data = {}
logger = logging.getLogger(__name__)


def run_web_service(progress_manager):
    """Runs the Flask web service."""
    global progress_data
    progress_data = progress_manager

    # Since this runs in a separate process, re-configure logging
    from dedupe_copy.logging_config import setup_logging

    setup_logging(verbosity="debug", use_colors=False)

    logger.info("Flask process started.")

    app = Flask(__name__)

    @app.route("/")
    def index():
        """Serves the main page with the Bokeh chart."""
        logger.info("Serving index page.")
        p = figure(
            x_range=[
                "Discovered",
                "Accepted",
                "Copied",
                "Deleted",
                "Ignored",
                "Errors",
            ],
            height=350,
            title="DedupeCopy Progress",
            toolbar_location=None,
            tools="",
        )
        p.vbar(
            x="x",
            top="top",
            width=0.9,
            source=ColumnDataSource(data=dict(x=[], top=[])),
            legend_label="Count",
            color=Category10_10[0],
        )

        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.yaxis.formatter = NumeralTickFormatter(format="0,0")

        script, div = components(p)

        html = f"""
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8">
                <title>DedupeCopy Progress</title>
                {script}
                <style>
                    body {{ font-family: sans-serif; }}
                    #stats {{ margin-bottom: 1em; }}
                </style>
            </head>
            <body>
                <h1>DedupeCopy Progress</h1>
                <div id="stats">
                    <p><strong>Status:</strong> <span id="status">Starting...</span></p>
                    <p><strong>Elapsed Time:</strong> <span id="elapsed">0s</span></p>
                    <p><strong>Files/Dirs:</strong> <span id="files">0</span>/<span id="dirs">0</span></p>
                    <p><strong>Last Accepted:</strong> <span id="last_accepted"></span></p>
                    <p><strong>Work/Result/Progress/Walk Queues:</strong>
                        <span id="work_q">0</span>/
                        <span id="result_q">0</span>/
                        <span id="progress_q">0</span>/
                        <span id="walk_q">0</span>
                    </p>
                </div>
                {div}
                <script>
                    function updateData() {{
                        fetch('/data')
                            .then(response => response.json())
                            .then(data => {{
                                const source = Bokeh.documents[0].get_model_by_name('{p.renderers[0].data_source.id}');
                                const new_data = {{
                                    x: ["Discovered", "Accepted", "Copied", "Deleted", "Ignored", "Errors"],
                                    top: [
                                        data.file_count,
                                        data.accepted_count,
                                        data.copied_count,
                                        data.deleted_count,
                                        data.ignored_count,
                                        data.error_count
                                    ]
                                }};
                                source.data = new_data;
                                source.change.emit();

                                // Update other stats
                                document.getElementById('status').textContent = data.status_message;
                                document.getElementById('elapsed').textContent = data.elapsed_time.toFixed(1) + 's';
                                document.getElementById('files').textContent = data.file_count;
                                document.getElementById('dirs').textContent = data.directory_count;
                                document.getElementById('last_accepted').textContent = data.last_accepted || '';
                                document.getElementById('work_q').textContent = data.work_queue_size;
                                document.getElementById('result_q').textContent = data.result_queue_size;
                                document.getElementById('progress_q').textContent = data.progress_queue_size;
                                document.getElementById('walk_q').textContent = data.walk_queue_size;
                            }});
                    }}
                    setInterval(updateData, 1000);
                </script>
            </body>
        </html>
        """
        return html

    @app.route("/data")
    def data():
        """Returns the progress data as JSON."""
        logger.debug("Serving data.")
        return jsonify(dict(progress_data))

    logger.info("Starting Flask server on 0.0.0.0:5000...")
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
