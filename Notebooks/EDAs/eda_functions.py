import numpy as np
import plotly.graph_objects as go
from sklearn.preprocessing import MinMaxScaler
from pyspark.sql.types import StructType, ArrayType, StructField



"""
This code for creating a football field plot inspired by the following link :.
Source: https://fcpython.com/visualisation/drawing-pitchmap-adding-lines-circles-matplotlib
We've adapted the code to use Plotly instead of Matplotlib.
"""


def create_football_field_plotly(ratio=1, legends=False):
    length = 120
    width = 80

    # Scale figure size
    fig = go.Figure(layout=go.Layout(width=1200 * ratio, height=800 * ratio))

    # Draw the field boundaries and center line
    fig.add_shape(type="rect", x0=0, y0=0, x1=length, y1=width, line=dict(color="black"))
    fig.add_shape(type="line", x0=length / 2, y0=0, x1=length / 2, y1=width, line=dict(color="black"))

    # Penalty areas
    fig.add_shape(type="rect", x0=0, y0=width / 2 - 18, x1=18, y1=width / 2 + 18, line=dict(color="black"))
    fig.add_shape(type="rect", x0=length - 18, y0=width / 2 - 18, x1=length, y1=width / 2 + 18,
                  line=dict(color="black"))

    # 6-yard boxes
    fig.add_shape(type="rect", x0=0, y0=width / 2 - 6, x1=6, y1=width / 2 + 6, line=dict(color="black"))
    fig.add_shape(type="rect", x0=length - 6, y0=width / 2 - 6, x1=length, y1=width / 2 + 6, line=dict(color="black"))

    # Circles: Center circle, center spot, and penalty spots
    fig.add_shape(type="circle", xref="x", yref="y", x0=length / 2 - 10, y0=width / 2 - 10, x1=length / 2 + 10,
                  y1=width / 2 + 10, line=dict(color="black"))
    fig.add_trace(go.Scatter(x=[length / 2], y=[width / 2], mode='markers', marker=dict(color='black', size=8)))
    fig.add_trace(
        go.Scatter(x=[12, length - 12], y=[width / 2, width / 2], mode='markers', marker=dict(color='black', size=8)))

    # Goals
    fig.add_shape(type="line", x0=0, y0=width / 2 - 4, x1=0, y1=width / 2 + 4, line=dict(color="black", width=4))
    fig.add_shape(type="line", x0=length, y0=width / 2 - 4, x1=length, y1=width / 2 + 4,
                  line=dict(color="black", width=4))

    # Remove axis and grid
    fig.update_layout(xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                      yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                      plot_bgcolor="green",
                      showlegend=legends)

    return fig



def add_scaled_size_crosses(fig, locations, counts):
    # Normalize counts using Min-Max normalization
    scaler = MinMaxScaler(feature_range=(8, 14))  # Set size range from 8 to 14
    scaled_sizes = scaler.fit_transform(np.array(counts).reshape(-1, 1)).flatten()

    # Add crosses with scaled sizes
    if locations is not None:
        fig.add_trace(go.Scatter(
            x=[loc[0] for loc in locations],
            y=[loc[1] for loc in locations],
            mode='markers',
            marker=dict(
                symbol='x',
                size=scaled_sizes,
                color='red'  # Fixed color for all markers
            ),
            text=[f'{count} counts' for count in counts],
            hoverinfo='text'
        ))

    return fig


def place_players_on_field(fig, positions_ids):
    height = 80
    width = 120
    n_axis_positions = 5

    y_gap = height / n_axis_positions
    x_gap = width / (n_axis_positions + 3)

    Ly = height - (y_gap / 2)
    LMy = Ly - y_gap
    MCy = Ly - 2 * y_gap
    RMy = Ly - 3 * y_gap
    Ry = Ly - 4 * y_gap

    Gx = x_gap
    Bx = 2 * x_gap
    DMx = 3 * x_gap
    CMx = 4 * x_gap
    AMx = 5 * x_gap
    SSx = 6 * x_gap
    Fx = 7 * x_gap

    positions_coordinates = {
        1: (Gx, MCy), 2: (Bx, Ry), 3: (Bx, RMy), 4: (Bx, MCy),
        5: (Bx, LMy), 6: (Bx, Ly), 7: (DMx, Ry), 8: (DMx, Ly),
        9: (DMx, RMy), 10: (DMx, MCy), 11: (DMx, LMy), 12: (CMx, Ry),
        13: (CMx, RMy), 14: (CMx, MCy), 15: (CMx, LMy), 16: (CMx, Ly),
        17: (AMx, Ry), 18: (AMx, RMy), 19: (AMx, MCy), 20: (AMx, LMy),
        21: (AMx, Ly), 22: (Fx, RMy), 23: (Fx, MCy), 24: (Fx, LMy),
        25: (SSx, MCy)
    }

    positions_names = {
        1: ['GK', 'Goalkeeper'], 2: ['RB', 'Right Back'], 3: ['RCB', 'Right Center Back'],
        4: ['CB', 'Center Back'], 5: ['LCB', 'Left Center Back'], 6: ['LB', 'Left Back'],
        7: ['RWB', 'Right Wing Back'], 8: ['LWB', 'Left Wing Back'], 9: ['RDM', 'Right Defensive Midfield'],
        10: ['CDM', 'Center Defensive Midfield'], 11: ['LDM', 'Left Defensive Midfield'], 12: ['RM', 'Right Midfield'],
        13: ['RCM', 'Right Center Midfield'], 14: ['CM', 'Center Midfield'], 15: ['LCM', 'Left Center Midfield'],
        16: ['LM', 'Left Midfield'], 17: ['RW', 'Right Wing'], 18: ['RAM', 'Right Attacking Midfield'],
        19: ['CAM', 'Center Attacking Midfield'], 20: ['LAM', 'Left Attacking Midfield'], 21: ['LW', 'Left Wing'],
        22: ['RCF', 'Right Center Forward'], 23: ['ST', 'Striker'], 24: ['LCF', 'Left Center Forward'],
        25: ['SS', 'Second Striker']
    }

    # Add player positions
    for id in positions_ids:
        x, y = positions_coordinates[id]
        abbreviation, entire_name = positions_names[id]
        fig.add_shape(type="circle", xref="x", yref="y", x0=x - 4, y0=y - 4, x1=x + 4, y1=y + 4,
                      line=dict(color="white"), )
        fig.add_trace(go.Scatter(x=[x], y=[y], text=[abbreviation], mode='text', textfont=dict(size=12, color="white"),
                                 textposition="middle center", name=f'{abbreviation} : {entire_name}', showlegend=True))

    for trace in fig.data[:2]:  # remove legends of the field plot
        trace.showlegend = False

    return fig


# Function to count features at each level of schema
def count_features(schema, level=0, counts=None):
    if counts is None:
        counts = {}
        sum_levels = {}

    counts[level] = counts.get(level, 0) + len(schema)

    for field in schema:
        if isinstance(field.dataType, StructType):
            count_features(field.dataType, level + 1, counts)
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            count_features(field.dataType.elementType, level + 1, counts)

    if level == 0:  # Sum and print counts only at the outermost call
        for level, count in counts.items():
            if level in sum_levels:
                sum_levels[level] += count
            else:
                sum_levels[level] = count

        for level, total_count in sum_levels.items():
            print(f'{total_count} features at level {level}')

    #return counts
