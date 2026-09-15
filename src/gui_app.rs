use std::{
    env,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    build_runtime, install_crypto_provider, DirectTopologyState, FrontendEvent, FrontendState,
    GatewayConfig, NetworkBackbone, NetworkControl, NetworkStatus, RestDispatcher, RestEvent,
    RestHandle, DEFAULT_INTENTS,
};
use eframe::egui;
use tokio::sync::broadcast;

const GATEWAY_EVENT_BUDGET: usize = 512;
const REST_EVENT_BUDGET: usize = 256;
const DEFAULT_HISTORY_PAGE: u8 = 50;

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
pub(crate) fn run_desktop() -> Result<(), eframe::Error> {
    install_crypto_provider();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1180.0, 760.0])
            .with_min_inner_size([760.0, 500.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Diddycord",
        options,
        Box::new(|creation_context| Box::new(DiddycordApp::new(creation_context))),
    )
}

#[cfg(target_os = "android")]
pub(crate) fn run_android(
    app: winit::platform::android::activity::AndroidApp,
) -> Result<(), eframe::Error> {
    use winit::platform::android::EventLoopBuilderExtAndroid as _;

    install_crypto_provider();

    // eframe 0.27 predates NativeOptions::android_app. Its supported escape hatch is the
    // event-loop builder hook, which lets us attach the AndroidApp required by winit 0.29.
    let mut options = eframe::NativeOptions::default();
    options.event_loop_builder = Some(Box::new(move |builder| {
        builder.with_android_app(app);
    }));

    eframe::run_native(
        "Diddycord",
        options,
        Box::new(|creation_context| Box::new(DiddycordApp::new(creation_context))),
    )
}

struct DiddycordApp {
    token: String,
    session: Option<GuiSession>,
    connect_error: Option<String>,
}

impl DiddycordApp {
    fn new(creation_context: &eframe::CreationContext<'_>) -> Self {
        #[cfg(target_os = "android")]
        {
            let mut style = (*creation_context.egui_ctx.style()).clone();
            style.spacing.interact_size.y = 44.0;
            style.spacing.item_spacing = egui::vec2(8.0, 8.0);
            style.spacing.button_padding = egui::vec2(10.0, 8.0);
            creation_context.egui_ctx.set_style(style);
        }

        #[cfg(not(target_os = "android"))]
        let _ = creation_context;

        Self::default()
    }
}

impl Default for DiddycordApp {
    fn default() -> Self {
        Self {
            token: env::var("DISCORD_BOT_TOKEN").unwrap_or_default(),
            session: None,
            connect_error: None,
        }
    }
}

impl Drop for DiddycordApp {
    fn drop(&mut self) {
        if let Some(session) = self.session.as_mut() {
            session.control.shutdown();
        }
    }
}

impl eframe::App for DiddycordApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.session.is_none() {
            self.show_login(ctx);
            return;
        }

        let disconnect = {
            let session = self
                .session
                .as_mut()
                .expect("session existence checked before graphical update");
            session.poll();

            #[cfg(target_os = "android")]
            {
                show_connected_mobile(ctx, session)
            }

            #[cfg(not(target_os = "android"))]
            {
                show_connected_desktop(ctx, session)
            }
        };

        if disconnect {
            if let Some(session) = self.session.as_mut() {
                session.control.shutdown();
            }
            self.session = None;
        } else {
            ctx.request_repaint_after(Duration::from_millis(50));
        }
    }
}

impl DiddycordApp {
    fn show_login(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                let top_space = (ui.available_height() * 0.14).max(20.0).min(100.0);
                ui.add_space(top_space);
                ui.heading("Diddycord");
                ui.label("Low-overhead graphical Discord bot client");
                ui.add_space(20.0);

                ui.group(|ui| {
                    ui.set_max_width(520.0);
                    ui.label("Discord bot token");
                    let token_edit = egui::TextEdit::singleline(&mut self.token)
                        .password(true)
                        .desired_width(f32::INFINITY)
                        .hint_text("Paste a bot token from the Discord Developer Portal");
                    ui.add(token_edit);
                    ui.add_space(8.0);
                    ui.small("Diddycord accepts bot/application tokens only. User-token/self-bot login is intentionally unsupported.");

                    if let Some(error) = self.connect_error.as_deref() {
                        ui.add_space(8.0);
                        ui.label(egui::RichText::new(error).strong());
                    }

                    ui.add_space(12.0);
                    let can_connect = !self.token.trim().is_empty();
                    if ui
                        .add_enabled(can_connect, egui::Button::new("Connect"))
                        .clicked()
                    {
                        let token = self.token.trim().to_owned();
                        match GuiSession::connect(token) {
                            Ok(session) => {
                                self.token.clear();
                                self.connect_error = None;
                                self.session = Some(session);
                            }
                            Err(error) => self.connect_error = Some(error),
                        }
                    }
                });
            });
        });
    }
}

struct GuiSession {
    control: NetworkControl,
    gateway_events: broadcast::Receiver<Arc<FrontendEvent>>,
    direct_events: broadcast::Receiver<Arc<FrontendEvent>>,
    rest_events: broadcast::Receiver<Arc<RestEvent>>,
    rest: RestHandle,
    state: FrontendState,
    direct: DirectTopologyState,
    worker: Option<JoinHandle<Result<(), String>>>,
    worker_finished: bool,
    selected_guild: Option<Box<str>>,
    direct_mode: bool,
    selected_channel: Option<Box<str>>,
    selected_channel_name: Option<Box<str>>,
    compose: String,
    notice: Option<String>,
}

impl GuiSession {
    fn connect(token: String) -> Result<Self, String> {
        if token.is_empty() {
            return Err("The bot token cannot be empty.".to_owned());
        }

        let backbone = NetworkBackbone::new(
            GatewayConfig::new(token.clone(), DEFAULT_INTENTS),
            GATEWAY_EVENT_BUDGET,
        );
        let control = backbone.control();
        let gateway_events = backbone.subscribe();
        let direct_events = backbone.subscribe();

        let (rest, rest_dispatcher) = RestDispatcher::new(&token, 64, REST_EVENT_BUDGET)
            .map_err(|error| error.to_string())?;
        let rest_events = rest.subscribe();

        let worker = thread::Builder::new()
            .name("diddycord-network".to_owned())
            .spawn(move || {
                let runtime = build_runtime().map_err(|error| error.to_string())?;
                runtime.block_on(async move {
                    let rest_task = tokio::spawn(rest_dispatcher.run());
                    let gateway_result = backbone.run().await.map_err(|error| error.to_string());
                    rest_task.abort();
                    gateway_result
                })
            })
            .map_err(|error| format!("failed to start network worker: {error}"))?;

        Ok(Self {
            control,
            gateway_events,
            direct_events,
            rest_events,
            rest,
            state: FrontendState::new(128, 200),
            direct: DirectTopologyState::default(),
            worker: Some(worker),
            worker_finished: false,
            selected_guild: None,
            direct_mode: false,
            selected_channel: None,
            selected_channel_name: None,
            compose: String::new(),
            notice: None,
        })
    }

    fn poll(&mut self) {
        self.state
            .drain(&mut self.gateway_events, GATEWAY_EVENT_BUDGET);
        self.direct
            .drain(&mut self.direct_events, GATEWAY_EVENT_BUDGET);
        self.state
            .drain_rest(&mut self.rest_events, REST_EVENT_BUDGET);

        let finished = self
            .worker
            .as_ref()
            .map(|worker| worker.is_finished())
            .unwrap_or(false);
        if finished {
            let worker = self.worker.take().expect("finished worker must exist");
            self.worker_finished = true;
            self.notice = match worker.join() {
                Ok(Ok(())) => Some("Network worker stopped.".to_owned()),
                Ok(Err(error)) => Some(error),
                Err(_) => Some("Network worker terminated unexpectedly.".to_owned()),
            };
        }
    }

    fn select_guild(&mut self, guild_id: Box<str>) {
        self.direct_mode = false;
        self.selected_guild = Some(guild_id);
        self.selected_channel = None;
        self.selected_channel_name = None;
    }

    fn select_direct_messages(&mut self) {
        self.direct_mode = true;
        self.selected_guild = None;
        self.selected_channel = None;
        self.selected_channel_name = None;
    }

    fn select_channel(&mut self, channel_id: Box<str>, name: Box<str>) {
        let changed = self.selected_channel.as_deref() != Some(channel_id.as_ref());
        self.selected_channel = Some(channel_id.clone());
        self.selected_channel_name = Some(name);
        if changed && self.state.channel_message_count(channel_id.as_ref()) == 0 {
            if let Err(error) = self
                .rest
                .try_fetch_messages(channel_id.as_ref(), DEFAULT_HISTORY_PAGE)
            {
                self.notice = Some(error.to_string());
            }
        }
    }

    fn back_to_channels(&mut self) {
        self.selected_channel = None;
        self.selected_channel_name = None;
        self.compose.clear();
    }

    fn back_to_servers(&mut self) {
        self.back_to_channels();
        self.selected_guild = None;
        self.direct_mode = false;
    }
}

#[cfg(not(target_os = "android"))]
fn show_connected_desktop(ctx: &egui::Context, session: &mut GuiSession) -> bool {
    let mut disconnect = false;

    egui::TopBottomPanel::top("diddycord_top_bar").show(ctx, |ui| {
        ui.horizontal(|ui| {
            ui.heading("Diddycord");
            ui.separator();
            ui.label(network_status_label(session.control.status()));

            if let Some(user) = session.control.self_user() {
                ui.separator();
                ui.label(format!("Signed in as {}", user.display_name()));
            }

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Disconnect").clicked() {
                    disconnect = true;
                }
            });
        });
    });

    egui::SidePanel::left("diddycord_guilds")
        .resizable(false)
        .default_width(170.0)
        .show(ctx, |ui| show_guilds(ui, session));

    egui::SidePanel::left("diddycord_channels")
        .resizable(true)
        .default_width(240.0)
        .width_range(180.0..=380.0)
        .show(ctx, |ui| show_channels(ui, session));

    egui::CentralPanel::default().show(ctx, |ui| show_messages(ui, session));

    disconnect
}

#[cfg(target_os = "android")]
fn show_connected_mobile(ctx: &egui::Context, session: &mut GuiSession) -> bool {
    let mut disconnect = false;
    let status = network_status_label(session.control.status());
    let self_name = session
        .control
        .self_user()
        .map(|user| user.display_name().to_owned());

    egui::TopBottomPanel::top("diddycord_mobile_top_bar").show(ctx, |ui| {
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            if session.selected_channel.is_some() {
                if ui.button("‹ Channels").clicked() {
                    session.back_to_channels();
                }
            } else if session.direct_mode || session.selected_guild.is_some() {
                if ui.button("‹ Servers").clicked() {
                    session.back_to_servers();
                }
            } else {
                ui.heading("Diddycord");
            }

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Disconnect").clicked() {
                    disconnect = true;
                }
            });
        });

        ui.horizontal_wrapped(|ui| {
            ui.small(status.as_str());
            if let Some(name) = self_name.as_deref() {
                ui.separator();
                ui.small(format!("Signed in as {name}"));
            }
        });
        ui.add_space(2.0);
    });

    egui::CentralPanel::default().show(ctx, |ui| {
        if session.selected_channel.is_some() {
            show_messages(ui, session);
        } else if session.direct_mode || session.selected_guild.is_some() {
            show_channels(ui, session);
        } else {
            show_guilds(ui, session);
        }
    });

    disconnect
}

fn show_guilds(ui: &mut egui::Ui, session: &mut GuiSession) {
    ui.heading("Servers");
    ui.separator();

    if ui
        .selectable_label(session.direct_mode, "Direct messages")
        .clicked()
    {
        session.select_direct_messages();
    }

    let guilds: Vec<(Box<str>, Box<str>, bool)> = session
        .state
        .topology()
        .guilds()
        .map(|guild| {
            (
                Box::<str>::from(guild.id),
                Box::<str>::from(guild.name),
                guild.unavailable,
            )
        })
        .collect();

    egui::ScrollArea::vertical().show(ui, |ui| {
        for (id, name, unavailable) in guilds {
            let selected =
                !session.direct_mode && session.selected_guild.as_deref() == Some(id.as_ref());
            let text = if unavailable {
                format!("{} (offline)", name)
            } else {
                name.to_string()
            };
            if ui.selectable_label(selected, text).clicked() {
                session.select_guild(id);
            }
        }
    });
}

fn show_channels(ui: &mut egui::Ui, session: &mut GuiSession) {
    if session.direct_mode {
        ui.heading("Direct messages");
        ui.separator();
        let channels: Vec<(Box<str>, Box<str>)> = session
            .direct
            .channels()
            .map(|channel| {
                let name = channel
                    .display_name()
                    .map(Box::<str>::from)
                    .unwrap_or_else(|| Box::<str>::from("Unknown recipient"));
                (channel.id.clone(), name)
            })
            .collect();

        egui::ScrollArea::vertical().show(ui, |ui| {
            for (id, name) in channels {
                let selected = session.selected_channel.as_deref() == Some(id.as_ref());
                if ui.selectable_label(selected, name.as_ref()).clicked() {
                    session.select_channel(id, name);
                }
            }
        });
        return;
    }

    let Some(guild_id) = session.selected_guild.as_deref() else {
        ui.heading("Channels");
        ui.separator();
        ui.label("Select a server or Direct messages.");
        return;
    };

    let guild_name = session
        .state
        .topology()
        .guild(guild_id)
        .map(|guild| guild.name.to_owned())
        .unwrap_or_else(|| "Channels".to_owned());
    ui.heading(guild_name);
    ui.separator();

    let mut channels: Vec<(Box<str>, Box<str>, u8, i32)> = session
        .state
        .topology()
        .channels(guild_id)
        .map(|channel| {
            let name = channel
                .name
                .as_ref()
                .cloned()
                .unwrap_or_else(|| Box::<str>::from("unnamed"));
            (channel.id.clone(), name, channel.kind, channel.position)
        })
        .collect();
    channels.sort_by(|left, right| left.3.cmp(&right.3).then_with(|| left.1.cmp(&right.1)));

    let threads: Vec<(Box<str>, Box<str>)> = session
        .state
        .topology()
        .threads(guild_id)
        .filter(|thread| !thread.archived)
        .map(|thread| {
            (
                thread.id.clone(),
                thread
                    .name
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| Box::<str>::from("thread")),
            )
        })
        .collect();

    egui::ScrollArea::vertical().show(ui, |ui| {
        for (id, name, kind, _) in channels {
            if kind == 4 {
                ui.add_space(6.0);
                ui.label(egui::RichText::new(name.as_ref()).strong());
                continue;
            }

            let text_capable = matches!(kind, 0 | 5);
            let selected = session.selected_channel.as_deref() == Some(id.as_ref());
            let label = if text_capable {
                format!("# {}", name)
            } else {
                format!("· {}", name)
            };
            let response =
                ui.add_enabled(text_capable, egui::SelectableLabel::new(selected, label));
            if response.clicked() {
                session.select_channel(id, name);
            } else if !text_capable {
                response.on_hover_text(
                    "This channel type is visible but is not a text timeline in the current GUI.",
                );
            }
        }

        if !threads.is_empty() {
            ui.add_space(10.0);
            ui.label(egui::RichText::new("Active threads").strong());
            for (id, name) in threads {
                let selected = session.selected_channel.as_deref() == Some(id.as_ref());
                if ui.selectable_label(selected, format!("↳ {name}")).clicked() {
                    session.select_channel(id, name);
                }
            }
        }
    });
}

fn show_messages(ui: &mut egui::Ui, session: &mut GuiSession) {
    let Some(channel_id) = session.selected_channel.as_ref().cloned() else {
        ui.vertical_centered(|ui| {
            ui.add_space((ui.available_height() * 0.35).max(24.0));
            ui.heading("Select a text channel");
            ui.label("Messages, live Gateway updates and REST history will appear here.");
        });
        return;
    };

    let title = session
        .selected_channel_name
        .as_deref()
        .unwrap_or(channel_id.as_ref());
    ui.heading(format!("# {title}"));
    ui.separator();

    let composer_height = if cfg!(target_os = "android") {
        150.0
    } else {
        110.0
    };
    let available_for_messages = (ui.available_height() - composer_height).max(120.0);
    egui::ScrollArea::vertical()
        .max_height(available_for_messages)
        .auto_shrink([false, false])
        .stick_to_bottom(true)
        .show(ui, |ui| {
            let mut count = 0usize;
            for message in session.state.messages(channel_id.as_ref()) {
                count += 1;
                ui.horizontal_wrapped(|ui| {
                    ui.label(egui::RichText::new(message.author_username.as_ref()).strong());
                    ui.label(message.content.as_ref());
                });
                ui.add_space(4.0);
            }
            if count == 0 {
                ui.label("No cached messages yet. Recent history is requested when a channel is selected.");
            }
        });

    ui.separator();
    if let Some(notice) = session.notice.as_deref() {
        ui.small(notice);
    }

    #[cfg(target_os = "android")]
    {
        let editor = egui::TextEdit::multiline(&mut session.compose)
            .desired_rows(2)
            .hint_text("Message this channel")
            .desired_width(f32::INFINITY);
        ui.add(editor);
        ui.horizontal(|ui| {
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                show_send_button(ui, session, channel_id.as_ref());
            });
        });
    }

    #[cfg(not(target_os = "android"))]
    ui.horizontal(|ui| {
        let editor = egui::TextEdit::multiline(&mut session.compose)
            .desired_rows(2)
            .hint_text("Message this channel")
            .desired_width(f32::INFINITY);
        ui.add(editor);
        show_send_button(ui, session, channel_id.as_ref());
    });
}

fn show_send_button(ui: &mut egui::Ui, session: &mut GuiSession, channel_id: &str) {
    let can_send = !session.compose.trim().is_empty() && !session.worker_finished;
    if ui
        .add_enabled(can_send, egui::Button::new("Send"))
        .clicked()
    {
        let content = session.compose.trim().to_owned();
        match session.rest.try_send_message(channel_id, &content) {
            Ok(_) => {
                session.compose.clear();
                session.notice = None;
            }
            Err(error) => session.notice = Some(error.to_string()),
        }
    }
}

fn network_status_label(status: NetworkStatus) -> String {
    match status {
        NetworkStatus::Idle => "Idle".to_owned(),
        NetworkStatus::Connecting { resume: true } => "Connecting (resume)".to_owned(),
        NetworkStatus::Connecting { resume: false } => "Connecting".to_owned(),
        NetworkStatus::Identifying => "Authenticating".to_owned(),
        NetworkStatus::Resuming => "Resuming session".to_owned(),
        NetworkStatus::Ready => "Connected".to_owned(),
        NetworkStatus::Reconnecting { attempt, resume } => {
            if resume {
                format!("Reconnecting #{attempt} (resume)")
            } else {
                format!("Reconnecting #{attempt}")
            }
        }
        NetworkStatus::Stopped => "Disconnected".to_owned(),
        NetworkStatus::Fatal { code } => format!("Disconnected (Gateway close {code})"),
    }
}
