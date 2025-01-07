import matplotlib.pyplot as plt
import matplotlib.animation as animation
import networkx as nx
import re
import time
import glob
import json
from collections import defaultdict
import colorsys
import os
import numpy as np

class DagVisualizer:
    def __init__(self, test_dir):
        self.test_dir = test_dir
        self.G = nx.DiGraph()
        self.pos = {}
        self.current_round = 0
        self.certificates_by_round = defaultdict(list)
        self.validator_colors = {}  # Map of validator ID to color
        self.last_read_timestamp = 0
        self.visible_rounds = 5  # Number of rounds to show
        self.max_stored_rounds = 10  # Maximum number of rounds to keep in memory
        self.node_cache = {}  # Cache for node
        self.edge_cache = {}  # Cache for edge
        self.last_update_time = time.time()
        self.update_interval = 2.0  # Minimum seconds between updates
        
        # Setup the plot
        plt.style.use('dark_background')
        self.fig, self.ax = plt.subplots(figsize=(16, 10))
        self.fig.patch.set_facecolor('#1C1C1C')
        self.ax.set_facecolor('#1C1C1C')
        
        # Setup tooltip with improved visibility
        self.tooltip = self.ax.annotate('', 
            xy=(0, 0), xytext=(20, 20), textcoords='offset points',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='black', alpha=0.8, edgecolor='white'),
            color='white',
            fontsize=10,
            zorder=100  # Ensure tooltip is drawn on top
        )
        self.tooltip.set_visible(False)
        self.fig.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        
    def get_validator_color(self, validator_id):
        if validator_id not in self.validator_colors:
            # Generate a new color using golden ratio for good distribution
            hue = len(self.validator_colors) * 0.618033988749895
            hue = hue - int(hue)
            self.validator_colors[validator_id] = colorsys.hsv_to_rgb(hue, 0.8, 0.95)
        return self.validator_colors[validator_id]
        
    def cleanup_old_rounds(self):
        # Keep only the last max_stored_rounds
        if self.certificates_by_round:
            rounds = sorted(self.certificates_by_round.keys())
            rounds_to_remove = rounds[:-self.max_stored_rounds] if len(rounds) > self.max_stored_rounds else []
            
            # Remove old rounds
            for round_num in rounds_to_remove:
                # Remove certificates from the graph
                for cert_id in self.certificates_by_round[round_num]:
                    if cert_id in self.G:
                        self.G.remove_node(cert_id)
                    if cert_id in self.pos:
                        del self.pos[cert_id]
                    if cert_id in self.node_cache:
                        del self.node_cache[cert_id]
                # Remove round from certificates_by_round
                del self.certificates_by_round[round_num]
            
            # Clear edge cache when removing nodes
            self.edge_cache.clear()

    def read_dag_state(self):
        # Throttle updates
        current_time = time.time()
        if current_time - self.last_update_time < self.update_interval:
            return False

        output_files = glob.glob(os.path.join(self.test_dir, "validator_*/primary/output.dag"))
        latest_state = None
        latest_timestamp = self.last_read_timestamp

        for output_file in output_files:
            try:
                with open(output_file, 'rb') as f:
                    try:
                        f.seek(-2, os.SEEK_END)
                        while f.read(1) != b'\n':
                            f.seek(-2, os.SEEK_CUR)
                    except OSError:
                        f.seek(0)
                    last_line = f.readline().decode()
                    
                    try:
                        state = json.loads(last_line.strip())
                        if state['timestamp'] > latest_timestamp:
                            latest_timestamp = state['timestamp']
                            latest_state = state
                    except json.JSONDecodeError:
                        continue
                            
            except FileNotFoundError:
                continue

        if latest_state and latest_timestamp > self.last_read_timestamp:
            self.last_read_timestamp = latest_timestamp
            self.current_round = latest_state['current_round']
            
            # Update graph
            self.G.clear()
            self.node_cache.clear()
            self.edge_cache.clear()
            
            min_round = max(0, self.current_round - self.visible_rounds + 1)
            filtered_vertices = [v for v in latest_state['vertices'] 
                               if v['round'] >= min_round]
            
            # First add all vertices
            for vertex in filtered_vertices:
                self.G.add_node(vertex['id'], 
                              round=vertex['round'],
                              author=vertex['author'],
                              incoming_edges=[],  # Will store nodes that point to this node
                              outgoing_edges=[])  # Will store nodes this node points to
            
            # Then process edges to count votes and parents
            visible_nodes = set(self.G.nodes())
            for edge in latest_state['edges']:
                if edge['from'] in visible_nodes and edge['to'] in visible_nodes:
                    self.G.add_edge(edge['from'], edge['to'])
                    # Add to incoming/outgoing edge lists
                    self.G.nodes[edge['to']]['incoming_edges'].append(edge['from'])
                    self.G.nodes[edge['from']]['outgoing_edges'].append(edge['to'])
            
            # Update certificates by round
            self.certificates_by_round.clear()
            for vertex in filtered_vertices:
                self.certificates_by_round[vertex['round']].append(vertex['id'])
            
            self.cleanup_old_rounds()
            self.last_update_time = current_time
            return True
        return False
    
    def update_layout(self):
        # Position nodes by round level, from bottom to top
        spacing_x = 3.0  # Increased horizontal spacing between nodes
        spacing_y = 1.5  # Vertical spacing between rounds
        
        # Get min and max rounds for scaling
        min_round = min(self.certificates_by_round.keys()) if self.certificates_by_round else 0
        
        for round_num, certs in self.certificates_by_round.items():
            # Normalize y position to be between 0 and 1
            y = (round_num - min_round) * spacing_y
            
            # Sort certificates by author to group them by validator
            sorted_certs = sorted(certs, key=lambda c: self.G.nodes[c]['author'])
            
            for i, cert_id in enumerate(sorted_certs):
                # Center certificates horizontally with consistent spacing
                x = (i - (len(certs) - 1) / 2) * spacing_x
                self.pos[cert_id] = (x, y)

    def draw_edges(self):
        if not self.edge_cache:
            edges = list(self.G.edges())
            if edges:
                nx.draw_networkx_edges(self.G, pos=self.pos, ax=self.ax,
                                     edge_color='#404040', arrows=True,
                                     arrowsize=15, arrowstyle='->',
                                     connectionstyle='arc3,rad=0',  # Reduced curve
                                     alpha=0.3, width=0.5)  # Thinner lines

    def draw_nodes(self):
        for node in self.G.nodes():
            if node not in self.node_cache:
                author = self.G.nodes[node]['author']
                color = self.get_validator_color(author)
                round_num = self.G.nodes[node]['round']
                pos_node = self.pos[node]
                
                # Draw node
                node_artist = nx.draw_networkx_nodes(self.G, pos=self.pos,
                                                   nodelist=[node],
                                                   node_color=[color],
                                                   node_size=1000,  # Smaller nodes
                                                   alpha=0.9,
                                                   edgecolors='white',
                                                   linewidths=1,
                                                   ax=self.ax)
                
                # Draw minimal label
                label_artist = plt.text(pos_node[0], pos_node[1], f"R{round_num}",
                                      horizontalalignment='center',
                                      verticalalignment='center',
                                      fontsize=6,  # Smaller font
                                      color='white',
                                      fontweight='bold')
                
                self.node_cache[node] = (node_artist, label_artist)

    def on_mouse_move(self, event):
        if event.inaxes != self.ax:
            self.tooltip.set_visible(False)
            return

        # Convert display coordinates to data coordinates
        transform = self.ax.transData.inverted()
        mouse_pos = transform.transform((event.x, event.y))

        # Find the closest node that exists in the graph
        min_dist = float('inf')
        closest_node = None
        for node, pos in self.pos.items():
            # Only consider nodes that still exist in the graph
            if node not in self.G:
                continue
            dist = np.sqrt((mouse_pos[0] - pos[0])**2 + (mouse_pos[1] - pos[1])**2)
            if dist < min_dist:
                min_dist = dist
                closest_node = node

        # Show tooltip if mouse is close enough to a node (threshold of 0.5 units)
        if min_dist < 0.5 and closest_node is not None and closest_node in self.G:
            try:
                node_data = self.G.nodes[closest_node]
                tooltip_text = f"ID: {closest_node[:6]}\n"
                tooltip_text += f"Round: {node_data['round']}\n"
                tooltip_text += f"Author: {node_data['author'][:6]}\n"
                
                # Add edge information
                incoming = node_data.get('incoming_edges', [])
                outgoing = node_data.get('outgoing_edges', [])
                tooltip_text += f"Incoming: {len(incoming)}\n"  # These are like votes
                tooltip_text += f"Parents: {len(outgoing)}"    # These are the parents

                # Update tooltip position and text
                node_pos = self.pos[closest_node]
                self.tooltip.xy = node_pos
                self.tooltip.set_text(tooltip_text)
                self.tooltip.set_visible(True)
                
                # Bring tooltip to front
                self.tooltip.set_zorder(1000)
                self.fig.canvas.draw()  # Force immediate redraw
            except (KeyError, AttributeError) as e:
                print(f"Error showing tooltip: {e}")
                self.tooltip.set_visible(False)
        else:
            self.tooltip.set_visible(False)
    
    def update(self, frame):
        # Only update if new data is available
        if self.read_dag_state():
            # Store tooltip state before clearing
            tooltip_visible = self.tooltip.get_visible()
            tooltip_text = self.tooltip.get_text()
            tooltip_position = self.tooltip.xy if tooltip_visible else None
            
            self.ax.clear()
            self.update_layout()
            
            if not self.G.nodes():
                self.ax.text(0.5, 0.5, 'Waiting for certificates...', 
                            ha='center', va='center', transform=self.ax.transAxes,
                            color='white', fontsize=12)
                return
            
            self.draw_edges()
            self.draw_nodes()
            
            # Add title
            self.ax.set_title(f'DAG Visualization - Round {self.current_round}',
                             color='white', pad=20, fontsize=14)
            
            # Simplified legend
            if self.validator_colors:
                legend_elements = [plt.Line2D([0], [0], marker='o', color='w',
                                            markerfacecolor=color, markersize=8,
                                            label=f'V{i+1}')
                                 for i, color in enumerate(self.validator_colors.values())]
                self.ax.legend(handles=legend_elements, loc='center left',
                             bbox_to_anchor=(1, 0.5), fontsize=8)
            
            # Set axis properties
            self.ax.set_xticks([])
            self.ax.set_yticks([])
            for spine in self.ax.spines.values():
                spine.set_visible(False)
            
            # Set fixed axis limits with padding
            all_pos = np.array(list(self.pos.values()))
            if len(all_pos) > 0:
                x_min, y_min = all_pos.min(axis=0) - 2
                x_max, y_max = all_pos.max(axis=0) + 2
                self.ax.set_xlim(x_min, x_max)
                self.ax.set_ylim(y_min, y_max)
            
            plt.tight_layout()
            
            # Recreate tooltip after clearing
            self.tooltip = self.ax.annotate('', 
                xy=(0, 0), xytext=(20, 20), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='black', alpha=0.8, edgecolor='white'),
                color='white',
                fontsize=10,
                zorder=100
            )
            
            # Restore tooltip state if it was visible
            if tooltip_visible and tooltip_position is not None:
                self.tooltip.xy = tooltip_position
                self.tooltip.set_text(tooltip_text)
                self.tooltip.set_visible(True)
                
            # Reconnect the mouse event handler
            self.fig.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
    
    def animate(self):
        ani = animation.FuncAnimation(self.fig, self.update, interval=1000)
        plt.show()

if __name__ == "__main__":
    import sys
    test_dir = sys.argv[1] if len(sys.argv) > 1 else "test"
    visualizer = DagVisualizer(test_dir)
    visualizer.animate() 