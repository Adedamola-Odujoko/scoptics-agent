# visualize_shots.py

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.lines import Line2D
import sys
import os
import json

def draw_pitch(ax, pitch_length=105, pitch_width=68):
    """Draws a regulation football pitch on a matplotlib axes object."""
    # Pitch background
    ax.add_patch(patches.Rectangle((-pitch_length / 2, -pitch_width / 2), pitch_length, pitch_width, facecolor='#4CAF50', zorder=0))

    # White lines
    line_color = 'white'
    
    # Outer lines and halfway line
    ax.plot([-pitch_length / 2, pitch_length / 2], [-pitch_width / 2, -pitch_width / 2], color=line_color)
    ax.plot([-pitch_length / 2, pitch_length / 2], [pitch_width / 2, pitch_width / 2], color=line_color)
    ax.plot([-pitch_length / 2, -pitch_length / 2], [-pitch_width / 2, pitch_width / 2], color=line_color)
    ax.plot([pitch_length / 2, pitch_length / 2], [-pitch_width / 2, pitch_width / 2], color=line_color)
    ax.plot([0, 0], [-pitch_width / 2, pitch_width / 2], color=line_color)
    
    # Center circle and penalty areas
    ax.add_patch(patches.Circle((0, 0), 9.15, edgecolor=line_color, facecolor='none'))
    ax.add_patch(patches.Circle((0, 0), 0.5, color=line_color))
    ax.add_patch(patches.Rectangle((-pitch_length / 2, -40.32 / 2), 16.5, 40.32, edgecolor=line_color, facecolor='none'))
    ax.add_patch(patches.Rectangle((pitch_length / 2 - 16.5, -40.32 / 2), 16.5, 40.32, edgecolor=line_color, facecolor='none'))
    
    # Goals
    goal_post_color = '#cccccc'
    ax.add_patch(patches.Rectangle((-pitch_length / 2 - 1.5, -7.32 / 2), 1.5, 7.32, edgecolor=goal_post_color, facecolor='none', lw=2))
    ax.add_patch(patches.Rectangle((pitch_length / 2, -7.32 / 2), 1.5, 7.32, edgecolor=goal_post_color, facecolor='none', lw=2))

    # Set limits and remove ticks
    ax.set_xlim(-pitch_length / 2 - 5, pitch_length / 2 + 5)
    ax.set_ylim(-pitch_width / 2 - 5, pitch_width / 2 + 5)
    ax.set_xticks([])
    ax.set_yticks([])

def plot_events(ax, df, team_color_map):
    """Plots all shots from the dataframe, styling them based on outcome."""
    if df.empty:
        return

    # Plot regular shots
    shots_df = df[df['outcome'] == 'Shot']
    for i, shot in shots_df.iterrows():
        team_color = team_color_map.get(shot['shooter_team'], '#FFFFFF')
        ax.scatter(shot['player_x'], shot['player_y'], color=team_color, edgecolor='white', marker='o', s=150, zorder=4)
        if pd.notna(shot['ball_path']):
            path = json.loads(shot['ball_path'])
            ax.plot([p[0] for p in path], [p[1] for p in path], color='#FFFF00', linestyle='-', lw=2, zorder=3)
        ax.text(shot['player_x'], shot['player_y'], f"{shot['game_minute']}:{shot['game_second']:02d}", color='white', fontsize=8, ha='center', va='center', zorder=5)

    # Plot blocked shots
    blocked_df = df[df['outcome'] == 'Blocked Shot']
    for i, shot in blocked_df.iterrows():
        team_color = team_color_map.get(shot['shooter_team'], '#FFFFFF')
        ax.scatter(shot['player_x'], shot['player_y'], color=team_color, edgecolor='white', marker='X', s=160, zorder=4)
        if pd.notna(shot['ball_path']):
            path = json.loads(shot['ball_path'])
            ax.plot([p[0] for p in path], [p[1] for p in path], color='#FF6347', linestyle='--', lw=2, zorder=3)
        ax.text(shot['player_x'], shot['player_y'], f"{shot['game_minute']}:{shot['game_second']:02d}", color='black', fontsize=8, ha='center', va='center', zorder=5)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python visualize_shots.py <match_id>")
        sys.exit(1)
    
    match_id = sys.argv[1]
    # The new, definitive filename
    csv_filename = f"final_classified_shots_match_{match_id}.csv"

    if not os.path.exists(csv_filename):
        print(f"Error: The file '{csv_filename}' was not found.")
        print("Please run the event detection script first to generate the final classified CSV.")
        sys.exit(1)

    df_shots = pd.read_csv(csv_filename)

    if df_shots.empty:
        print("The CSV file is empty. No shots to plot.")
        sys.exit(0)

    # --- Create the Plot ---
    fig, ax = plt.subplots(figsize=(14, 9))
    fig.set_facecolor('#333333')
    draw_pitch(ax)

    # --- Define Team Colors ---
    # Create a unique list of teams and assign colors
    teams = df_shots['shooter_team'].unique()
    colors = ['#6CABDD', '#C8102E'] # Man City Blue, Liverpool Red
    team_color_map = {team: color for team, color in zip(teams, colors)}

    # Plot all events using our new helper function
    plot_events(ax, df_shots, team_color_map)
    
    # --- Create Custom Legend ---
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='Shot (On/Off Target)', markerfacecolor='#999999', markersize=12),
        Line2D([0], [0], marker='X', color='w', label='Blocked Shot', markerfacecolor='#999999', markersize=12),
        Line2D([0], [0], color='#FFFF00', lw=2, linestyle='-', label='Shot Trajectory'),
        Line2D([0], [0], color='#FF6347', lw=2, linestyle='--', label='Blocked Trajectory')
    ]
    # Add team colors to the legend
    for team, color in team_color_map.items():
        legend_elements.append(patches.Patch(facecolor=color, edgecolor='w', label=team))
    
    ax.legend(handles=legend_elements, loc='upper right', frameon=True, facecolor='#555555', labelcolor='white')
    
    ax.set_title(f"Shot and Block Analysis for Match {match_id} (n={len(df_shots)})", color='white', fontsize=18, fontweight='bold')
    plt.tight_layout()
    output_filename = f"shot_analysis_match_{match_id}.png"
    plt.savefig(output_filename, dpi=300, facecolor=fig.get_facecolor())
    
    print(f"Successfully generated new analysis map: {output_filename}")