# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import numpy as np


def timeseries(
        plot_ax,
        datalake,
        datastream_key,
        data_key,
        label,
        color=None,
        line_kwargs=None,
        shade_kwargs=None,
        shade=True,
    ):
    """
    Plot a datastream from the datalake.

    Parameters
    ----------
    plot_ax : matplotlib.axes._subplots.AxesSubplot
        The axis to plot on.
    datalake : DataLake
        The datalake to plot from.
    datastream_key : str
        The key of the datastream to plot.
    data_key : str
        The key of the data to plot.
    label : str
        The label for the plot.
    color : str, optional
        The color of the plot, by default None
    line_kwargs : dict
        The keyword arguments for the line plot.
    shade_kwargs : dict
        The keyword arguments for the shaded area plot.
    """
    if line_kwargs is None:
        line_kwargs = {}
    if shade_kwargs is None:
        shade_kwargs = {'alpha': 0.25}
    if color is None:
        color = plot_ax._get_lines.get_next_color()
    
    time = datalake.datastreams[datastream_key].return_time(datetime64=True)
    data = datalake.datastreams[datastream_key].return_data(keys=[data_key])[0]
    
    if shade:
        std = datalake.datastreams[datastream_key].return_std(keys=[data_key])[0]
        # plot shaded area
        plot_ax.fill_between(
            time,
            data *(1-std/data),
            data *(1+std/data),
            color=color,
            **shade_kwargs
        )
    # plot line
    plot_ax.plot(
        time,
        data,
        color=color,
        label=label,
        **line_kwargs
    )
