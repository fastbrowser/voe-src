slint::slint! {
    import { Button, VerticalBox } from "std-widgets.slint";

    export component AppWindow inherits Window {
        in-out property <float> progress: 0.0;
        in-out property <string> status: "Ready to deploy VOE Client";
        in-out property <bool> install_enabled: true;

        callback request_install();

        title: "VOE Client Installer";
        width: 400px;
        height: 200px;
        background: #1e1e1e;

        VerticalBox {
            padding: 30px;
            spacing: 20px;
            alignment: center;

            Text {
                text: root.status;
                color: #cccccc;
                horizontal-alignment: center;
                font-size: 14px;
            }

            Rectangle {
                width: 300px;
                height: 20px;
                background: #333333;
                border-radius: 5px;
                Rectangle {
                    x: 0px;
                    width: root.progress * parent.width;
                    height: parent.height;
                    background: #007acc;
                    border-radius: 5px;
                }
            }

            Button {
                text: "Install Now";
                width: 150px;
                enabled: root.install_enabled;
                clicked => {
                    root.request_install();
                }
            }
        }
    }
}

use std::fs;
use std::path::{PathBuf};
use std::env;

const CLIENT_BINARY: &[u8] = include_bytes!("../../target/x86_64-pc-windows-gnu/release/client.exe");
const CLIENT_CONFIG: &[u8] = include_bytes!("../../config-client-shipping.yml");

fn main() -> Result<(), slint::PlatformError> {
    let ui = AppWindow::new()?;
    let ui_handle = ui.as_weak();

    ui.on_request_install(move || {
        let ui_handle_clone = ui_handle.clone();
        
        std::thread::spawn(move || {
            let update_ui = |status: &str, prog: f32, enabled: bool| {
                let ui_handle_inner = ui_handle_clone.clone();
                let s = status.to_string();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_handle_inner.upgrade() {
                        ui.set_status(s.into());
                        ui.set_progress(prog);
                        ui.set_install_enabled(enabled);
                    }
                });
            };

            update_ui("Calculating paths...", 0.1, false);
            
            let local_app_data = match env::var("LOCALAPPDATA") {
                Ok(val) => val,
                Err(_) => {
                    update_ui("Error: LOCALAPPDATA not found", 0.0, true);
                    return;
                }
            };
            let install_dir = PathBuf::from(local_app_data).join("Voe");
            
            if let Err(e) = fs::create_dir_all(&install_dir) {
                update_ui(&format!("Folder Error: {}", e), 0.0, true);
                return;
            }

            update_ui("Extracting client.exe...", 0.4, false);
            let exe_path = install_dir.join("client.exe");
            if let Err(e) = fs::write(&exe_path, CLIENT_BINARY) {
                update_ui(&format!("Binary Error: {}", e), 0.0, true);
                return;
            }

            update_ui("Extracting config-client.yml...", 0.7, false);
            let config_path = install_dir.join("config-client.yml");
            if let Err(e) = fs::write(&config_path, CLIENT_CONFIG) {
                update_ui(&format!("Config Error: {}", e), 0.0, true);
                return;
            }

            update_ui("Creating Start Menu link...", 0.9, false);
            let app_data = match env::var("APPDATA") {
                Ok(val) => val,
                Err(_) => {
                    update_ui("Error: APPDATA not found", 0.0, true);
                    return;
                }
            };
            let shortcut_dir = PathBuf::from(app_data).join("Microsoft\\Windows\\Start Menu\\Programs\\Voe");
            
            if let Err(e) = fs::create_dir_all(&shortcut_dir) {
                update_ui(&format!("Shortcut Folder Error: {}", e), 0.0, true);
                return;
            }

            let shortcut_path = shortcut_dir.join("VOE Client.bat");
            let batch_content = format!(
                "@echo off\nstart \"\" \"{}\"\nexit",
                exe_path.to_string_lossy()
            );

            if let Err(e) = fs::write(&shortcut_path, batch_content) {
                update_ui(&format!("Shortcut Error: {}", e), 0.0, true);
                return;
            }

            update_ui("Installation Complete!", 1.0, false);
        });
    });

    ui.run()
}