import pandas as pd
import iso8601

from bokeh.io import curdoc, show
from bokeh.layouts import row
from bokeh.models import Range1d, DatetimeTickFormatter, LinearAxis, HoverTool
from bokeh.plotting import figure

from star.bokeh.load_data import load_sentiment_data
from star.config.config_name_map import *
from star.db.connector import DBConnector
from star.utils.config_utils import StarConfig


def create_graph(sentiments, history, ticker):
    hover = HoverTool(
        tooltips=[
            ("Date", "$index"),
            ("Cumulative sentiment", "$y")
        ]
    )

    _plot = figure(title="Twitter Sentiment vs Market Close values", plot_width=1600, plot_height=800,
                   x_axis_type="datetime", tools=[hover])
    _plot.y_range = Range1d(0, history.Close.max() + 10)
    _plot.xaxis.axis_label = "Date"
    _plot.yaxis.axis_label = "Close Price"
    _plot.xaxis.formatter = DatetimeTickFormatter(
        hours=["%d %B %Y"],
        days=["%d %B %Y"],
        months=["%d %B %Y"],
        years=["%d %B %Y"],
    )
    _plot.extra_y_ranges = {"sentiment": Range1d(start=-2, end=110)}

    _plot.line(x=history.Date, y=history.Close, legend=ticker, line_color='blue', line_width=2)
    _plot.circle(x=history.Date, y=history.Close, fill_color="white", line_color="blue", size=6)

    # Adding the second axis to the plot.
    _plot.add_layout(LinearAxis(y_range_name="sentiment"), 'right')
    _plot.line(x=sentiments.index, y=sentiments.bearish, legend='bearish', line_color='brown', line_width=2,
               y_range_name="sentiment")
    _plot.circle(x=sentiments.index, y=sentiments.bearish, fill_color="white", line_color="brown", size=6,
                 y_range_name="sentiment")

    _plot.line(x=sentiments.index, y=sentiments.undetermined, legend='undetermined', line_color='black', line_width=2,
               y_range_name="sentiment")
    _plot.circle(x=sentiments.index, y=sentiments.undetermined, fill_color="white", line_color="black", size=6,
                 y_range_name="sentiment")

    _plot.line(x=sentiments.index, y=sentiments.bullish, legend='bullish', line_color='red', line_width=2,
               y_range_name="sentiment")
    _plot.circle(x=sentiments.index, y=sentiments.bullish, fill_color="white", line_color="red", size=6,
                 y_range_name="sentiment")

    return _plot


def create_db_connectors(config):
    _uri = config[DB_TYPE][DB_URI]
    _db = config[DB_TYPE][DB_NAME]
    _sentiments = config[DB_TYPE][SENTIMENTS]
    return {'sentiments': DBConnector(DB_TYPE, _uri, _db, _sentiments)}


def get_config_values(config_path):
    return StarConfig(config_path, 'yaml').parse()


config = get_config_values("/home/andy/PycharmProjects/STAR/star/config/star.yml")
db_con = create_db_connectors(config)
s, h = load_sentiment_data(['JNUG'], iso8601.parse_date('2014-12-01'), iso8601.parse_date('2015-01-01'), db_con)
plot = create_graph(s, h, 'JNUG')
show(plot)

curdoc().add_root(row(plot))
curdoc().title = "witter Sentiment vs Market Close values"
