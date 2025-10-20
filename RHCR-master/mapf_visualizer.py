import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from matplotlib.patches import Rectangle, Circle
from matplotlib.collections import LineCollection
import matplotlib.colors as mcolors
import os
import math

class GeneralizedKivaVisualizer:
    def __init__(self, map_file='kiva.map', results_file='my_results_paths.txt'):
        print("Loading Generalized Kiva Warehouse MAPF data...")
        
        # Load data
        self.map_data = self.load_kiva_map(map_file)
        self.grid_height, self.grid_width = self.map_data.shape
        self.raw_data = self.load_raw_data(results_file)
        self.solution = self.convert_to_solution()
        
        # **ADAPTIVE SETUP: Configure based on robot count**
        self.num_agents = len(self.solution['agents'])
        self.setup_adaptive_parameters()
        
        # Pre-cache warehouse statistics
        self.warehouse_stats = {
            'shelves': np.sum(self.map_data == '@'),
            'endpoints': np.sum(self.map_data == 'e'),
            'robot_zones': np.sum(self.map_data == 'r'),
            'free_space': np.sum(self.map_data == '.')
        }
        
        # Setup visualization
        self.setup_adaptive_layout()
        self.current_timestep = 0
        self.max_timestep = max(len(path) for path in self.solution['agents'].values()) if self.solution['agents'] else 1
        
        # Pre-render static elements
        self.setup_static_warehouse()
        self.agent_artists = []
        self.trail_artists = []
        
        print(f"SUCCESS: Generalized visualizer ready for {self.num_agents} robots")
        print(f"Mode: {self.viz_mode} | Grid: {self.grid_width}x{self.grid_height}")

    def setup_adaptive_parameters(self):
        """Configure visualization parameters based on robot count"""
        
        if self.num_agents <= 10:
            self.viz_mode = "DETAILED"
            self.robot_size = 0.4
            self.font_size = 10
            self.trail_length = 12
            self.trail_alpha = 0.7
            self.show_individual_status = True
            self.show_robot_ids = True
            self.grid_detail = "high"
            
        elif self.num_agents <= 30:
            self.viz_mode = "MEDIUM"
            self.robot_size = 0.3
            self.font_size = 8
            self.trail_length = 8
            self.trail_alpha = 0.5
            self.show_individual_status = True
            self.show_robot_ids = True
            self.grid_detail = "medium"
            
        elif self.num_agents <= 100:
            self.viz_mode = "COMPACT"
            self.robot_size = 0.25
            self.font_size = 6
            self.trail_length = 5
            self.trail_alpha = 0.4
            self.show_individual_status = False
            self.show_robot_ids = True
            self.grid_detail = "low"
            
        elif self.num_agents <= 500:
            self.viz_mode = "DENSE"
            self.robot_size = 0.15
            self.font_size = 4
            self.trail_length = 3
            self.trail_alpha = 0.3
            self.show_individual_status = False
            self.show_robot_ids = False
            self.grid_detail = "minimal"
            
        else:  # 500+ robots
            self.viz_mode = "HEATMAP"
            self.robot_size = 0.1
            self.font_size = 0
            self.trail_length = 2
            self.trail_alpha = 0.2
            self.show_individual_status = False
            self.show_robot_ids = False
            self.grid_detail = "none"
        
        # **ADAPTIVE COLOR SCHEME**
        self.setup_adaptive_colors()
        
        # **ADAPTIVE ANIMATION SPEED**
        if self.num_agents <= 10:
            self.animation_interval = 300
        elif self.num_agents <= 50:
            self.animation_interval = 400
        elif self.num_agents <= 200:
            self.animation_interval = 500
        else:
            self.animation_interval = 600

    def setup_adaptive_colors(self):
        """Generate colors that work for any number of robots"""
        if self.num_agents <= 12:
            # Use distinct colors for small groups
            self.colors = plt.cm.Set3(np.linspace(0, 1, max(self.num_agents, 1)))
        elif self.num_agents <= 50:
            # Combine multiple colormaps
            colors = []
            maps = ['Set3', 'Dark2', 'Paired', 'Accent']
            per_map = math.ceil(self.num_agents / len(maps))
            
            for i, cmap_name in enumerate(maps):
                start_idx = i * per_map
                end_idx = min((i + 1) * per_map, self.num_agents)
                if start_idx < self.num_agents:
                    cmap = plt.cm.get_cmap(cmap_name)
                    colors.extend([cmap(j / per_map) for j in range(end_idx - start_idx)])
            
            self.colors = np.array(colors[:self.num_agents])
        else:
            # Use continuous color space for large numbers
            self.colors = plt.cm.rainbow(np.linspace(0, 1, self.num_agents))

    def setup_adaptive_layout(self):
        """Setup figure layout based on visualization mode"""
        if self.viz_mode in ["DETAILED", "MEDIUM"]:
            # Two-panel layout with info
            self.fig, (self.ax_main, self.ax_info) = plt.subplots(1, 2, figsize=(20, 12))
            self.use_info_panel = True
        elif self.viz_mode in ["COMPACT", "DENSE"]:
            # Main plot with minimal info overlay
            self.fig, self.ax_main = plt.subplots(1, 1, figsize=(16, 12))
            self.use_info_panel = False
        else:  # HEATMAP mode
            # Full screen visualization
            self.fig, self.ax_main = plt.subplots(1, 1, figsize=(18, 14))
            self.use_info_panel = False

    def setup_static_warehouse(self):
        """Pre-render static warehouse elements"""
        self.ax_main.set_xlim(-0.5, self.grid_width - 0.5)
        self.ax_main.set_ylim(-0.5, self.grid_height - 0.5)
        self.ax_main.set_aspect('equal')
        
        # **ADAPTIVE WAREHOUSE RENDERING**
        if self.grid_detail == "high":
            # Full detail for small robot counts
            self.render_detailed_warehouse()
        elif self.grid_detail == "medium":
            # Simplified rendering
            self.render_medium_warehouse()
        elif self.grid_detail == "low":
            # Basic shapes only
            self.render_basic_warehouse()
        elif self.grid_detail == "minimal":
            # Just obstacles
            self.render_minimal_warehouse()
        else:  # "none"
            # Background color coding only
            self.render_background_only()
        
        # **ADAPTIVE GRID LINES**
        if self.grid_detail in ["high", "medium"]:
            spacing = 5 if self.grid_detail == "high" else 10
            for x in range(0, self.grid_width, spacing):
                self.ax_main.axvline(x - 0.5, color='lightgray', linewidth=0.2, alpha=0.5)
            for y in range(0, self.grid_height, spacing):
                self.ax_main.axhline(y - 0.5, color='lightgray', linewidth=0.2, alpha=0.5)

    def render_detailed_warehouse(self):
        """Full warehouse detail for ≤10 robots"""
        for y in range(self.grid_height):
            for x in range(self.grid_width):
                cell = self.map_data[y, x]
                
                if cell == '@':
                    rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='saddlebrown', edgecolor='black', alpha=0.9)
                    self.ax_main.add_patch(rect)
                elif cell == 'e':
                    rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='orange', edgecolor='darkorange', alpha=0.8)
                    self.ax_main.add_patch(rect)
                    self.ax_main.text(x, y, 'E', ha='center', va='center', fontweight='bold', fontsize=6, color='white')
                elif cell == 'r':
                    rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='lightblue', edgecolor='blue', alpha=0.6)
                    self.ax_main.add_patch(rect)
                    self.ax_main.text(x, y, 'R', ha='center', va='center', fontweight='bold', fontsize=6, color='darkblue')

    def render_medium_warehouse(self):
        """Simplified warehouse for 10-30 robots"""
        for y in range(self.grid_height):
            for x in range(self.grid_width):
                cell = self.map_data[y, x]
                
                if cell == '@':
                    rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='brown', alpha=0.8)
                    self.ax_main.add_patch(rect)
                elif cell == 'e':
                    rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='orange', alpha=0.7)
                    self.ax_main.add_patch(rect)
                elif cell == 'r':
                    rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='lightblue', alpha=0.5)
                    self.ax_main.add_patch(rect)

    def render_basic_warehouse(self):
        """Basic warehouse for 30-100 robots"""
        # Batch render for performance
        shelf_coords = np.where(self.map_data == '@')
        endpoint_coords = np.where(self.map_data == 'e')
        robot_coords = np.where(self.map_data == 'r')
        
        for y, x in zip(shelf_coords[0], shelf_coords[1]):
            rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='brown', alpha=0.6)
            self.ax_main.add_patch(rect)
            
        for y, x in zip(endpoint_coords[0], endpoint_coords[1]):
            rect = Rectangle((x-0.5, y-0.5), 1, 1, facecolor='orange', alpha=0.5)
            self.ax_main.add_patch(rect)

    def render_minimal_warehouse(self):
        """Minimal warehouse for 100-500 robots"""
        # Only show obstacles as scatter plot for performance
        shelf_coords = np.where(self.map_data == '@')
        if len(shelf_coords[0]) > 0:
            self.ax_main.scatter(shelf_coords[1], shelf_coords[0], c='brown', s=4, alpha=0.5, marker='s')
        
        endpoint_coords = np.where(self.map_data == 'e')
        if len(endpoint_coords[0]) > 0:
            self.ax_main.scatter(endpoint_coords[1], endpoint_coords[0], c='orange', s=4, alpha=0.5, marker='s')

    def render_background_only(self):
        """Background color coding for 500+ robots"""
        # Create background image for maximum performance
        background = np.ones((self.grid_height, self.grid_width, 3))
        
        shelf_mask = self.map_data == '@'
        endpoint_mask = self.map_data == 'e'
        robot_mask = self.map_data == 'r'
        
        background[shelf_mask] = [0.6, 0.4, 0.2]  # Brown
        background[endpoint_mask] = [1.0, 0.6, 0.0]  # Orange
        background[robot_mask] = [0.7, 0.8, 1.0]  # Light blue
        
        self.ax_main.imshow(background, extent=[-0.5, self.grid_width-0.5, self.grid_height-0.5, -0.5], alpha=0.5)

    def update_dynamic_elements(self, timestep):
        """Adaptive robot rendering based on count"""
        # Clear previous elements
        for artist in self.agent_artists + self.trail_artists:
            artist.remove()
        self.agent_artists.clear()
        self.trail_artists.clear()
        
        if self.viz_mode == "HEATMAP":
            return self.render_heatmap_mode(timestep)
        else:
            return self.render_individual_robots(timestep)

    def render_individual_robots(self, timestep):
        """Render robots individually"""
        robot_status = []
        active_count = 0
        picking_count = 0
        completed_count = 0
        
        for agent_id, path in self.solution['agents'].items():
            if timestep < len(path):
                x, y = path[timestep]
                color = self.colors[agent_id % len(self.colors)]
                
                # Draw robot
                circle = Circle((x, y), self.robot_size, facecolor=color, edgecolor='black', linewidth=1)
                self.ax_main.add_patch(circle)
                self.agent_artists.append(circle)
                
                # Robot ID (if enabled)
                if self.show_robot_ids and self.font_size > 0:
                    text = self.ax_main.text(x, y, str(agent_id), ha='center', va='center',
                                           fontweight='bold', fontsize=self.font_size, color='white')
                    self.agent_artists.append(text)
                
                # Trail
                if timestep > 0 and self.trail_length > 0:
                    trail_len = min(self.trail_length, timestep)
                    if trail_len > 1:
                        trail_points = [path[t] for t in range(timestep-trail_len, timestep)]
                        trail_x, trail_y = zip(*trail_points)
                        line, = self.ax_main.plot(trail_x, trail_y, color=color, alpha=self.trail_alpha, linewidth=1)
                        self.trail_artists.append(line)
                
                # Status tracking
                cell_type = self.map_data[y, x] if 0 <= y < self.grid_height and 0 <= x < self.grid_width else '?'
                if cell_type == 'e':
                    picking_count += 1
                    if self.show_individual_status:
                        robot_status.append(f"R{agent_id}: ({x},{y}) PICKING")
                    # Highlight
                    star = self.ax_main.plot(x, y, '*', color='yellow', markersize=max(6, 12-self.num_agents//10), markeredgecolor='black')[0]
                    self.agent_artists.append(star)
                else:
                    active_count += 1
                    if self.show_individual_status:
                        robot_status.append(f"R{agent_id}: ({x},{y}) ACTIVE")
            else:
                completed_count += 1
                if self.show_individual_status:
                    robot_status.append(f"R{agent_id}: COMPLETED")
        
        # Update title with summary
        summary = f"Active: {active_count} | Picking: {picking_count} | "
        self.ax_main.set_title(f'Kiva Warehouse ({self.num_agents} robots) - {summary} - Step: {timestep}', 
                              fontsize=12, fontweight='bold')
        
        return robot_status, {'active': active_count, 'picking': picking_count, 'completed': completed_count}

    def render_heatmap_mode(self, timestep):
        """Heatmap visualization for 500+ robots"""
        # Create density heatmap
        density_map = np.zeros((self.grid_height, self.grid_width))
        
        for agent_id, path in self.solution['agents'].items():
            if timestep < len(path):
                x, y = path[timestep]
                if 0 <= x < self.grid_width and 0 <= y < self.grid_height:
                    density_map[y, x] += 1
        
        # Clear previous heatmap
        for artist in self.agent_artists:
            artist.remove()
        self.agent_artists.clear()
        
        # Draw heatmap
        if np.max(density_map) > 0:
            im = self.ax_main.imshow(density_map, cmap='hot', alpha=0.7, 
                                   extent=[-0.5, self.grid_width-0.5, self.grid_height-0.5, -0.5])
            self.agent_artists.append(im)
        
        active_robots = np.sum(density_map)
        self.ax_main.set_title(f'Kiva Warehouse Heatmap - {int(active_robots)} Active Robots - Step: {timestep}', 
                              fontsize=12, fontweight='bold')
        
        return [], {'active': int(active_robots), 'picking': 0, 'completed': self.num_agents - int(active_robots)}

    def update_info_panel(self, timestep, robot_status, summary_stats):
        """Adaptive info panel"""
        if not self.use_info_panel:
            return
            
        self.ax_info.clear()
        self.ax_info.set_xlim(0, 10)
        self.ax_info.set_ylim(0, 10)
        self.ax_info.axis('off')
        
        if self.viz_mode == "DETAILED":
            # Full detail mode
            info_text = f"""KIVA WAREHOUSE - DETAILED VIEW
================================
Grid: {self.grid_width}x{self.grid_height}
Robots: {self.num_agents} (Mode: {self.viz_mode})

Warehouse:
• Shelves: {self.warehouse_stats['shelves']}
• Endpoints: {self.warehouse_stats['endpoints']}
• Robot Zones: {self.warehouse_stats['robot_zones']}

Status Summary:
• Active: {summary_stats['active']}
• Picking: {summary_stats['picking']}
• Completed: {summary_stats['completed']}

Timestep: {timestep}/{self.max_timestep-1}

Individual Robots:
{chr(10).join(robot_status[:15])}
{f"... and {len(robot_status) - 15} more" if len(robot_status) > 15 else ""}"""

        else:  # MEDIUM mode
            # Summary mode
            info_text = f"""KIVA WAREHOUSE - SUMMARY VIEW
=============================
{self.num_agents} Robots (Mode: {self.viz_mode})
Grid: {self.grid_width}x{self.grid_height}

ROBOT STATUS:
Active: {summary_stats['active']}
Picking: {summary_stats['picking']}
Completed: {summary_stats['completed']}

PROGRESS:
Timestep: {timestep}/{self.max_timestep-1}
Progress: {timestep/self.max_timestep*100:.1f}%

WAREHOUSE:
Shelves: {self.warehouse_stats['shelves']}
Endpoints: {self.warehouse_stats['endpoints']}

Top 8 Robots:
{chr(10).join(robot_status[:8])}"""
        
        font_size = 9 if self.viz_mode == "DETAILED" else 8
        self.ax_info.text(0.1, 9.8, info_text, fontsize=font_size, ha='left', va='top',
                         fontfamily='monospace', 
                         bbox=dict(boxstyle="round,pad=0.3", facecolor="lightcyan", alpha=0.8))

    def animate(self, frame):
        """Generalized animation function"""
        self.current_timestep = frame
        robot_status, summary_stats = self.update_dynamic_elements(frame)
        
        if self.use_info_panel:
            self.update_info_panel(frame, robot_status, summary_stats)
        
        return self.agent_artists + self.trail_artists

    # Include all the previous data loading methods (load_kiva_map, load_raw_data, etc.)
    # [Previous methods remain the same - keeping the response concise]

    def generate_demo_data(self):
        """Generate demo data for any number of robots"""
        demo_data = {}
        
        # Find positions
        robot_positions = np.where(self.map_data == 'r')
        endpoint_positions = np.where(self.map_data == 'e')
        
        robot_locs = [(y * self.grid_width + x, x, y) for y, x in zip(robot_positions[0], robot_positions[1])]
        endpoint_locs = [(y * self.grid_width + x, x, y) for y, x in zip(endpoint_positions[0], endpoint_positions[1])]
        
        # **ADAPTIVE: Generate appropriate number of demo robots**
        if not robot_locs:  # If no robot positions in map, create distributed starts
            num_demo_robots = min(self.num_agents, 50) if self.num_agents > 0 else 25
        else:
            num_demo_robots = min(self.num_agents, len(robot_locs) * 3) if self.num_agents > 0 else min(25, len(robot_locs))
        
        for agent_id in range(num_demo_robots):
            # Distribute starting positions
            if robot_locs:
                start_loc, start_x, start_y = robot_locs[agent_id % len(robot_locs)]
            else:
                # Create grid distribution for many robots
                start_x = 1 + (agent_id % int(math.sqrt(self.grid_width - 2)))
                start_y = 1 + (agent_id // int(math.sqrt(self.grid_width - 2)))
                start_x = min(start_x, self.grid_width - 2)
                start_y = min(start_y, self.grid_height - 2)
            
            # Distribute goal positions
            if endpoint_locs:
                goal_loc, goal_x, goal_y = endpoint_locs[(agent_id * 7) % len(endpoint_locs)]
            else:
                goal_x = self.grid_width - 2 - (agent_id % 5)
                goal_y = self.grid_height - 2 - (agent_id // 5)
            
            # Variable path lengths for realistic movement
            base_length = 30
            variation = agent_id % 20
            path_length = base_length + variation
            
            path = []
            for t in range(path_length):
                progress = min(1.0, t / (path_length * 0.8))
                x = int(start_x + (goal_x - start_x) * progress)
                y = int(start_y + (goal_y - start_y) * progress)
                
                # Add movement variation
                if t % 3 == 0 and t > 5:
                    x += (agent_id % 3) - 1
                    y += ((agent_id + t) % 3) - 1
                    x = max(0, min(x, self.grid_width - 1))
                    y = max(0, min(y, self.grid_height - 1))
                
                location_id = y * self.grid_width + x
                path.append((location_id, t))
            
            demo_data[agent_id] = path
        
        print(f"Generated demo data for {num_demo_robots} robots")
        return demo_data

    # [Include all other methods from previous versions]
    def load_kiva_map(self, map_file):
        """Load Kiva warehouse map"""
        if not os.path.exists(map_file):
            print(f"ERROR: Map file {map_file} not found!")
            return np.array([['.' for _ in range(46)] for _ in range(33)])
        
        with open(map_file, 'r') as f:
            lines = f.readlines()
        
        try:
            header = lines[0].strip().split(',')
            map_height, map_width = int(header[0]), int(header[1])
            
            map_grid = []
            for i in range(4, 4 + map_height):
                if i < len(lines):
                    row = lines[i].strip()[:map_width].ljust(map_width, '.')
                    map_grid.append(list(row))
                else:
                    map_grid.append(['.'] * map_width)
            
            return np.array(map_grid)
            
        except Exception as e:
            print(f"Error parsing map: {e}")
            return np.array([['.' if (x+y) % 3 != 0 else '@' for x in range(46)] for y in range(33)])

    def load_raw_data(self, results_file):
        """Load RHCR results"""
        if not os.path.exists(results_file):
            return self.generate_demo_data()
        
        try:
            with open(results_file, 'r') as f:
                lines = f.readlines()
            
            num_agents = int(lines[0].strip())
            raw_data = {}
            
            for agent_id in range(num_agents):
                if agent_id + 1 < len(lines):
                    line = lines[agent_id + 1].strip()
                    positions = line.split(';')
                    
                    agent_data = []
                    for pos in positions:
                        if pos.strip():
                            try:
                                parts = pos.split(',')
                                location_id = int(parts[0])
                                timestep = int(parts[2])
                                agent_data.append((location_id, timestep))
                            except:
                                continue
                    
                    raw_data[agent_id] = sorted(agent_data, key=lambda x: x[1])
            
            return raw_data
            
        except Exception as e:
            print(f"Error loading results: {e}")
            return self.generate_demo_data()

    def location_to_xy(self, location_id):
        """Convert location ID to x,y coordinates"""
        return location_id % self.grid_width, location_id // self.grid_width

    def convert_to_solution(self):
        """Convert to solution format"""
        solution = {'agents': {}}
        
        for agent_id, agent_data in self.raw_data.items():
            agent_path = []
            for location_id, timestep in agent_data:
                x, y = self.location_to_xy(location_id)
                x = max(0, min(x, self.grid_width - 1))
                y = max(0, min(y, self.grid_height - 1))
                agent_path.append((x, y))
            
            solution['agents'][agent_id] = agent_path
        
        return solution

    def run(self):
        """Start generalized visualization"""
        print(f"\nStarting Generalized Kiva Warehouse Visualization")
        print(f"Robots: {self.num_agents} | Mode: {self.viz_mode}")
        print(f"Animation: {self.animation_interval}ms intervals")
        
        mode_descriptions = {
            "DETAILED": "Full detail with individual robot tracking",
            "MEDIUM": "Simplified view with robot IDs", 
            "COMPACT": "Compact view for moderate robot counts",
            "DENSE": "Minimal detail for large robot counts",
            "HEATMAP": "Density heatmap for massive robot swarms"
        }
        
        print(f"Description: {mode_descriptions.get(self.viz_mode, 'Unknown mode')}")
        print("Close window to exit")
        
        anim = animation.FuncAnimation(
            self.fig, self.animate, frames=self.max_timestep,
            interval=self.animation_interval,
            blit=False,
            repeat=True,
            cache_frame_data=False
        )
        
        plt.tight_layout()
        plt.show()
        return anim

def main():
    print("=" * 70)
    print("    GENERALIZED KIVA WAREHOUSE VISUALIZER")
    print("    Automatically adapts to any number of robots")
    print("=" * 70)
    
    try:
        visualizer = GeneralizedKivaVisualizer('kiva.map', 'my_results_paths.txt')
        visualizer.run()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
