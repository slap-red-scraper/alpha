import kivy
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.core.window import Window
from kivy.uix.gridlayout import GridLayout
from kivy.uix.checkbox import CheckBox
from kivy.uix.scrollview import ScrollView
from kivy.clock import Clock
from src.logger import Logger
from src.config import ConfigLoader
from kivy.properties import BooleanProperty # For NavButton is_active state
# from src.main import execute_scraping_logic as actual_run_scraper_main # Already imported later
import threading
import configparser
import os

# Define NavButton Python side for the custom property, Kivy will link it to KV rule <NavButton@Button>
class NavButton(Button):
    is_active = BooleanProperty(False)

class ConfigScreen(Screen):
    def __init__(self, **kwargs):
        super(ConfigScreen, self).__init__(**kwargs)
        self.inputs = {}
        # The layout and widgets for ConfigScreen are more dynamic and will remain mostly in Python.
        # KV styles for Label, TextInput, Button, CheckBox will apply.
        self._create_layout()

    def _create_layout(self):
        # This layout creation remains in Python due to its dynamic nature based on config fields
        # However, padding and spacing will come from KV's <GridLayout> rule if not overridden here.
        # Or, assign a class/id and style it in KV: self.layout.style_class = 'ConfigGridLayout'
        self.layout_grid = GridLayout(cols=2) # padding/spacing from KV
        
        # Load initial configuration
        self.config_loader = ConfigLoader(path="config.ini")
        try:
            if not os.path.exists("config.ini"):
                self.app_config = None
                # Use a label for warnings within the UI if possible, or rely on console print
                print("WARNING: config.ini not found. GUI will show empty fields or defaults.")
                # self.layout_grid.add_widget(Label(text="config.ini not found. Using defaults."))
            else:
                self.app_config = self.config_loader.load()
        except Exception as e: # Catch potential errors from ConfigLoader.load()
            self.app_config = None
            print(f"ERROR: Failed to load config.ini: {e}")
            # self.layout_grid.add_widget(Label(text=f"Error loading config: {e}"))


        # Define fields to be created
        fields = [
            ("Mobile:", "mobile", "credentials.mobile", False, False),
            ("Password:", "password", "credentials.password", True, False),
            ("URL File:", "url_file", "settings.url_file", False, False),
            ("Downline Enabled:", "downline_enabled", "settings.downline_enabled", False, True),
            ("Log File:", "log_file", "logging.log_file", False, False),
            ("Log Level:", "log_level", "logging.log_level", False, False),
            ("Console Logging:", "console", "logging.console", False, True),
            ("Log Detail:", "detail", "logging.detail", False, False),
        ]

        for label_text, key, config_path, is_password, is_checkbox in fields:
            self.layout_grid.add_widget(Label(text=label_text))
            
            # Resolve path to get value from app_config
            current_value = ""
            if self.app_config:
                try:
                    value_keys = config_path.split('.')
                    temp_val = self.app_config
                    for k in value_keys:
                        if isinstance(temp_val, dict): # If it's a dict from a sub-config
                             temp_val = temp_val.get(k)
                        else: # If it's a dataclass
                            temp_val = getattr(temp_val, k)
                    current_value = temp_val
                except (AttributeError, KeyError):
                    current_value = False if is_checkbox else "" # Default for missing keys

            if is_checkbox:
                widget = CheckBox(active=bool(current_value))
            else:
                widget = TextInput(
                    text=str(current_value),
                    password=is_password,
                    write_tab=False
                )
            self.inputs[key] = widget
            self.layout_grid.add_widget(widget)

        # Save Button - height from KV if Button rule has it, or set here if specific
        self.save_button = Button(text='Save Configuration') # size_hint_y, height from KV
        self.save_button.bind(on_press=self.save_config)
        self.layout_grid.add_widget(Label()) # Placeholder
        self.layout_grid.add_widget(self.save_button)

        # Status Label - height from KV if Label rule has it, or set here
        self.config_status_label = Label(text='') # height from KV
        self.layout_grid.add_widget(self.config_status_label)
        # Add an empty widget to fill the grid if status label doesn't span
        self.layout_grid.add_widget(Label()) 


        self.add_widget(self.layout_grid)

    def save_config(self, instance):
        # Access status_label via self.config_status_label now
        config_parser = configparser.ConfigParser()

        config_parser['credentials'] = {
            'mobile': self.inputs['mobile'].text,
            'password': self.inputs['password'].text
        }
        config_parser['settings'] = {
            'file': self.inputs['url_file'].text,
            'downline': str(self.inputs['downline_enabled'].active)
        }
        config_parser['logging'] = {
            'log_file': self.inputs['log_file'].text,
            'log_level': self.inputs['log_level'].text,
            'console': str(self.inputs['console'].active),
            'detail': self.inputs['detail'].text
        }

        try:
            with open('config.ini', 'w') as configfile:
                config_parser.write(configfile)
            self.config_status_label.text = "Configuration Saved!"
            # Optionally, clear the message after a few seconds
            Clock.schedule_once(lambda dt: setattr(self.config_status_label, 'text', ""), 5)
        except Exception as e:
            self.config_status_label.text = f"Error saving: {e}"
            print(f"Error saving configuration: {e}")


class ProgressScreen(Screen):
    # KV file now defines the layout. Python class is for logic.
    # __init__ can be minimal or used for non-widget related setup.
    # on_kv_post is useful if you need to access self.ids right after KV parsing.
    
    # Example: If start_button's on_press needs to be bound in Python
    # def on_kv_post(self, base_widget):
    #    self.ids.start_button.bind(on_press=lambda x: App.get_running_app().start_scraping_thread(self.ids.start_button))
    # However, it's better to define this in the KV file if possible:
    # <ProgressScreen>:
    #     BoxLayout:
    #         Button:
    #             id: start_button
    #             on_press: app.start_scraping_thread(self) # 'self' here is the button

    def add_log_message(self, message):
        # Ensure messages are handled on the main thread
        # Check if self.ids exists, which means KV rules have been applied.
        if hasattr(self, 'ids') and 'log_display' in self.ids:
            log_display_widget = self.ids.log_display
            def append_message(msg):
                log_display_widget.text += msg + "\n"
                if hasattr(self.ids, 'log_display_scroll'):
                    Clock.schedule_once(lambda dt: setattr(self.ids.log_display_scroll, 'scroll_y', 0), 0)
            
            if threading.current_thread() != threading.main_thread():
                Clock.schedule_once(lambda dt: append_message(message))
            else:
                append_message(message)
        else:
            # Fallback or error if called before ids are populated (should not happen if KV is correct)
            print(f"WARN: ProgressScreen.add_log_message called before ids populated or log_display not in ids. Message: {message}")

            
    def clear_logs(self):
        if hasattr(self, 'ids') and 'log_display' in self.ids and 'status_label' in self.ids:
            self.ids.log_display.text = ""
            self.ids.status_label.text = ""
        else:
            print("WARN: ProgressScreen.clear_logs called before ids populated.")


    def set_status(self, message):
        if hasattr(self, 'ids') and 'status_label' in self.ids:
            status_label_widget = self.ids.status_label
            def update_status(msg):
                status_label_widget.text = msg

            if threading.current_thread() != threading.main_thread():
                Clock.schedule_once(lambda dt: update_status(message))
            else:
                update_status(message)
        else:
            print(f"WARN: ProgressScreen.set_status called before ids populated. Message: {message}")


class HistoryScreen(Screen):
    # KV file defines the layout. Python class is for logic.
    # __init__ can be minimal.
    # on_kv_post can be used for bindings if not done in KV.
    # e.g. self.ids.refresh_button.bind(on_press=self.load_and_display_metrics)
    # Or in KV: <Button>: id: refresh_button; on_press: root.load_and_display_metrics()

    def __init__(self, **kwargs):
        super(HistoryScreen, self).__init__(**kwargs)
        self.metric_labels = {} 

    def display_metrics(self, metrics_data):
        if not hasattr(self, 'ids') or 'metrics_layout' not in self.ids:
            print("WARN: HistoryScreen.display_metrics called before ids populated or metrics_layout not in ids.")
            return
            
        metrics_layout_widget = self.ids.metrics_layout
        metrics_layout_widget.clear_widgets() 
        self.metric_labels.clear()

        # Define the order and display names for metrics
        metric_display_order = {
            "runs": "Total Scraper Runs:",
            "total_runtime": "Total Runtime (seconds):",
            "bonuses": "Total Bonuses Fetched:",
            "total_bonus_amount": "Total Bonus Amount:",
            "downlines": "Total Downlines Fetched:",
            "errors": "Total Errors Logged:",
            "successful_bonus_fetches": "Successful Bonus Fetches (Events):",
            "failed_bonus_api_calls": "Failed Bonus API Calls:"
        }

        for key, display_name in metric_display_order.items():
            value = metrics_data.get(key, "N/A") 
            
            name_label = Label(text=display_name, halign='left') # size_hint_x from KV if needed via class rule
            # name_label.bind(texture_size=name_label.setter('size')) # KV can handle this with size_hint
            
            value_label = Label(text=str(value), halign='right')
            # value_label.bind(texture_size=value_label.setter('size'))

            metrics_layout_widget.add_widget(name_label)
            metrics_layout_widget.add_widget(value_label)
            self.metric_labels[key] = value_label

    def load_and_display_metrics(self, instance=None): 
        metrics_layout_widget = self.ids.metrics_layout
        try:
            cfg_loader = ConfigLoader() 
            app_cfg = cfg_loader.load() 
            log_file_path = app_cfg.logging.log_file
            logger_instance = Logger(log_file=log_file_path, log_level='INFO', console=False, detail='LESS')
            metrics = logger_instance.load_metrics(log_file_path)
            self.display_metrics(metrics)
        except FileNotFoundError:
            metrics_layout_widget.clear_widgets()
            metrics_layout_widget.add_widget(Label(text="Error: config.ini not found.", color=(1,0,0,1)))
            print("Error: config.ini not found. Cannot load metrics.")
        except KeyError as e:
            metrics_layout_widget.clear_widgets()
            metrics_layout_widget.add_widget(Label(text=f"Config Error: Missing key {e}.", color=(1,0,0,1)))
            print(f"Error: Configuration missing key {e}. Cannot load metrics.")
        except Exception as e:
            metrics_layout_widget.clear_widgets()
            metrics_layout_widget.add_widget(Label(text=f"Error: {e}", color=(1,0,0,1)))
            print(f"An unexpected error occurred: {e}")

    def on_enter(self):
        self.load_and_display_metrics()

class ScraperApp(App):
    # Primary and Highlight colors for Window and other Python-side styling if needed.
    # These are also defined in KV for KV-side styling.
    primary_bg_color = (0.133, 0.133, 0.133, 1)
    highlight_color = (1, 0.2, 0.2, 1)

    def build(self):
        Window.clearcolor = self.primary_bg_color
        Window.minimum_width = 800
        Window.minimum_height = 600
        # Title is already set in KV for ScraperApp if we define a root rule for it,
        # or keep it here. For now, it's set in python.
        Window.title = "Scraper Control Panel"


        # The root layout. KV file will define its structure if we use a root rule.
        # For now, Python builds the main structure (Nav + ScreenManager).
        # KV will style the NavButtons and Screens.
        root_layout = BoxLayout(orientation='vertical') # No padding/spacing here, let KV handle it or use a class
        
        self.screen_manager = ScreenManager()
        
        self.config_screen = ConfigScreen(name='config')
        self.progress_screen = ProgressScreen(name='progress')
        self.history_screen = HistoryScreen(name='history')
        
        self.screen_manager.add_widget(self.config_screen)
        self.screen_manager.add_widget(self.progress_screen) 
        self.screen_manager.add_widget(self.history_screen)
        
        # Navigation Bar
        self.nav_bar = BoxLayout(orientation='horizontal', size_hint_y=None, height='50dp') # Height from KV
        
        self.nav_buttons = {
            'config': NavButton(text='Configuration'),
            'progress': NavButton(text='Progress'),
            'history': NavButton(text='History')
        }
        
        self.nav_buttons['config'].bind(on_press=lambda x: self.switch_screen('config'))
        self.nav_buttons['progress'].bind(on_press=lambda x: self.switch_screen('progress'))
        self.nav_buttons['history'].bind(on_press=lambda x: self.switch_screen('history'))

        for name, button in self.nav_buttons.items():
            self.nav_bar.add_widget(button)
        
        root_layout.add_widget(self.nav_bar)
        root_layout.add_widget(self.screen_manager) 
        
        self.switch_screen('config') # Start on config screen
        return root_layout

    def switch_screen(self, screen_name):
        self.screen_manager.current = screen_name
        for name, button in self.nav_buttons.items():
            button.is_active = (name == screen_name)

    def start_scraping_thread(self, button_instance):
        # Ensure button_instance is the one from self.ids if called from KV
        # If called via lambda from Python-created button, it's direct.
        # The ProgressScreen KV has id 'start_button'.
        # So if ProgressScreen itself binds it, it should use self.ids.start_button.
        # If ScraperApp binds it (as it does now via lambda in ProgressScreen KV for example),
        # the button instance is passed correctly.
        # The 'button_instance' argument IS the button pressed.
        
        if button_instance:
            button_instance.disabled = True
        else: # Fallback if somehow not passed, though KV should pass it
            fallback_button = self.progress_screen.ids.get('start_button')
            if fallback_button:
                fallback_button.disabled = True

        self.progress_screen.clear_logs()
        self.progress_screen.set_status("INFO: Starting scraping process...")
        # Ensure this method is idempotent or handles multiple clicks if necessary
        # For now, assume button disable/enable handles rapid clicks.
        
        button_instance.disabled = True
        self.progress_screen.clear_logs()
        self.progress_screen.set_status("INFO: Starting scraping process...")
        
        log_callback = self.progress_screen.add_log_message

        # Import the refactored main function
        from src.main import execute_scraping_logic as actual_run_scraper_main 

        def target_for_thread():
            try:
                actual_run_scraper_main(gui_callback=log_callback)
                self.progress_screen.set_status("INFO: Scraping process completed.")
            except Exception as e:
                log_callback(f"ERROR: Scraping thread failed: {e}")
                self.progress_screen.set_status(f"ERROR: Scraping failed: {e}")
                # Potentially log the full traceback to the GUI log as well
                import traceback
                log_callback(f"TRACEBACK: {traceback.format_exc()}")
            finally:
                # Re-enable button on the main Kivy thread
                if button_instance:
                    Clock.schedule_once(lambda dt: setattr(button_instance, 'disabled', False))
                else: # Fallback
                    fallback_button = self.progress_screen.ids.get('start_button')
                    if fallback_button:
                        Clock.schedule_once(lambda dt: setattr(fallback_button, 'disabled', False))
                
                # Optionally, refresh history screen data
                if self.history_screen:
                     Clock.schedule_once(lambda dt: self.history_screen.load_and_display_metrics())


        thread = threading.Thread(target=target_for_thread)
        thread.daemon = True # Allow main app to exit even if thread is running
        thread.start()

if __name__ == '__main__':
    # Remove the TestApp, use the main ScraperApp
    ScraperApp().run()
