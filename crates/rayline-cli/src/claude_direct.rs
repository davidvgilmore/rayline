use std::fs;
use std::net::TcpListener;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::claude::RunRequest;

const MAX_OUTPUT_TOKENS_ENV: &str = "CLAUDE_CODE_MAX_OUTPUT_TOKENS";
const MAX_CONTEXT_TOKENS_ENV: &str = "CLAUDE_CODE_MAX_CONTEXT_TOKENS";

struct Plan {
    root: PathBuf,
    config: Vec<u8>,
    ports: [u16; 4],
    provider_keys: Vec<String>,
    max_output_tokens: Option<NonZeroU64>,
    max_context_tokens: Option<NonZeroU64>,
}

fn plan(request: &RunRequest) -> Result<Plan, String> {
    plan_with_token_limits(
        request,
        std::env::var_os(MAX_OUTPUT_TOKENS_ENV),
        std::env::var_os(MAX_CONTEXT_TOKENS_ENV),
    )
}

fn positive_token_limit(
    name: &str,
    value: Option<std::ffi::OsString>,
) -> Result<Option<NonZeroU64>, String> {
    value
        .map(|value| {
            value
                .to_str()
                .filter(|value| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|value| value.parse::<NonZeroU64>().ok())
                .ok_or_else(|| format!("{name} must be a positive decimal u64 integer"))
        })
        .transpose()
}

fn plan_with_token_limits(
    request: &RunRequest,
    output_tokens: Option<std::ffi::OsString>,
    context_tokens: Option<std::ffi::OsString>,
) -> Result<Plan, String> {
    let max_output_tokens = positive_token_limit(MAX_OUTPUT_TOKENS_ENV, output_tokens)?;
    let max_context_tokens = positive_token_limit(MAX_CONTEXT_TOKENS_ENV, context_tokens)?;
    let root = request
        .fresh_profile
        .as_ref()
        .ok_or("--fresh-profile is required")?;
    if !root.is_absolute()
        || root.exists()
        || root
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err("--fresh-profile must be a new absolute directory without '..'".into());
    }
    if request
        .model
        .as_deref()
        .is_some_and(|model| model != "rayline-arc")
    {
        return Err("direct ARC sessions require model rayline-arc".into());
    }
    let path = request.config_path.as_ref().ok_or("--config is required")?;
    let config = fs::read(path).map_err(|e| format!("read ARC configuration: {e}"))?;
    let parsed: rayline_local_router::RouterConfig = serde_json::from_slice(&config)
        .map_err(|e| format!("invalid router configuration: {e}"))?;
    let arc = parsed
        .arc
        .as_ref()
        .ok_or("direct mode requires an ARC configuration")?;
    if arc.session.is_none() || arc.bindings.is_empty() {
        return Err("direct mode requires an ARC session service and action bindings".into());
    }
    if arc.bindings.values().any(|b| b.target.endpoint == "local") {
        return Err(
            "direct ARC bindings require named endpoints; no bundled generator is started".into(),
        );
    }
    for arg in &request.args {
        let arg = arg.to_string_lossy();
        let name = arg.split('=').next().unwrap_or_default();
        if matches!(
            name,
            "--model"
                | "-m"
                | "--settings"
                | "--setting-sources"
                | "--mcp-config"
                | "--strict-mcp-config"
                | "--chrome"
                | "--resume"
                | "--continue"
                | "-c"
                | "--agent"
                | "--agents"
                | "--fallback-model"
        ) {
            return Err(format!(
                "{name} is incompatible with a fresh direct ARC profile"
            ));
        }
    }
    let listeners: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<Result<_, _>>()
        .map_err(|e| format!("allocate local ports: {e}"))?;
    let mut ports = [0; 4];
    for (slot, listener) in ports.iter_mut().zip(listeners.iter()) {
        *slot = listener.local_addr().map_err(|e| e.to_string())?.port();
    }
    Ok(Plan {
        root: root.clone(),
        config,
        ports,
        max_output_tokens,
        max_context_tokens,
        provider_keys: parsed
            .endpoints
            .iter()
            .filter_map(|endpoint| endpoint.api_key_env.clone())
            .collect(),
    })
}

fn private_directory(path: &Path) -> Result<(), String> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|e| format!("create private directory {}: {e}", path.display()))
}

fn child_environment(command: &mut Command, provider: bool) {
    for (name, _) in std::env::vars_os() {
        let key = name.to_string_lossy().to_ascii_uppercase();
        if key.starts_with("RAYLINE_")
            || key.starts_with("ANTHROPIC_")
            || key.starts_with("CLAUDE_")
            || matches!(
                key.as_str(),
                "HTTP_PROXY"
                    | "HTTPS_PROXY"
                    | "ALL_PROXY"
                    | "NO_PROXY"
                    | "NODE_OPTIONS"
                    | "NODE_EXTRA_CA_CERTS"
            )
            || (!provider
                && (key.starts_with("OPENAI_")
                    || key.starts_with("AWS_")
                    || key.starts_with("GOOGLE_")
                    || key.starts_with("VERTEX_")))
        {
            command.env_remove(name);
        }
    }
}

fn daemon_command(binary: &Path, plan: &Plan) -> Command {
    daemon_command_with_env(binary, plan, |name| std::env::var_os(name))
}

fn daemon_command_with_env(
    binary: &Path,
    plan: &Plan,
    provider_env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Command {
    let mut command = Command::new(binary);
    child_environment(&mut command, true);
    for name in &plan.provider_keys {
        if let Some(value) = provider_env(name) {
            command.env(name, value);
        }
    }
    command
        .args([
            "serve",
            "--no-local-model",
            "--decision-plane",
            "local",
            "--router-config-path",
        ])
        .arg(plan.root.join("router.json"))
        .arg("--data-dir")
        .arg(plan.root.join("rld"));
    for (flag, port) in [
        "--local-router-port",
        "--adapter-port",
        "--injector-port",
        "--metrics-port",
    ]
    .into_iter()
    .zip(plan.ports)
    {
        command.arg(flag).arg(port.to_string());
    }
    command.stdin(Stdio::null());
    command
}

fn client_command(binary: &Path, request: &RunRequest, plan: &Plan) -> Command {
    let mut command = Command::new(binary);
    child_environment(&mut command, false);
    for name in &plan.provider_keys {
        command.env_remove(name);
    }
    command
        .env("CLAUDE_CONFIG_DIR", plan.root.join("claude"))
        .env(
            "ANTHROPIC_BASE_URL",
            format!("http://127.0.0.1:{}", plan.ports[0]),
        )
        .env("ANTHROPIC_API_KEY", "rayline-local")
        .env("ANTHROPIC_MODEL", "rayline-arc")
        .env("ANTHROPIC_CUSTOM_MODEL_OPTION", "rayline-arc")
        .env(
            "ANTHROPIC_CUSTOM_MODEL_OPTION_NAME",
            "Rayline ARC — model routing",
        )
        .env(
            "ANTHROPIC_CUSTOM_MODEL_OPTION_DESCRIPTION",
            "ARC selects the configured model endpoint for each turn.",
        )
        .env("ANTHROPIC_DEFAULT_OPUS_MODEL", "rayline-arc")
        .env("ANTHROPIC_DEFAULT_SONNET_MODEL", "rayline-arc")
        .env("ANTHROPIC_DEFAULT_HAIKU_MODEL", "rayline-arc")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        .env("CLAUDE_CODE_DISABLE_AGENT_VIEW", "1")
        .args([
            "--bare",
            "--name",
            "Rayline ARC",
            "--model",
            "rayline-arc",
            "--setting-sources",
            "",
            "--settings",
        ])
        .arg(plan.root.join("settings.json"))
        .args(["--strict-mcp-config", "--mcp-config"])
        .arg(plan.root.join("mcp.json"))
        .args(["--no-chrome", "--no-session-persistence"])
        .args(&request.args);
    // Apply only the validated planning snapshot after the customization scrub.
    command.env_remove(MAX_OUTPUT_TOKENS_ENV);
    if let Some(tokens) = plan.max_output_tokens {
        command.env(MAX_OUTPUT_TOKENS_ENV, tokens.to_string());
    }
    command.env_remove(MAX_CONTEXT_TOKENS_ENV);
    if let Some(tokens) = plan.max_context_tokens {
        command.env(MAX_CONTEXT_TOKENS_ENV, tokens.to_string());
    }
    if let Some(tokens) = request.auto_compact_window {
        command.env(crate::claude::AUTO_COMPACT_WINDOW_ENV, tokens.to_string());
    }
    command
}

struct OwnedChild(tokio::process::Child, u32);
impl OwnedChild {
    fn spawn(mut command: Command) -> Result<Self, String> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut command = tokio::process::Command::from(command);
        command.kill_on_drop(true);
        let child = command.spawn().map_err(|e| format!("launch child: {e}"))?;
        let pid = child.id().ok_or("child process has no PID")?;
        Ok(Self(child, pid))
    }
    async fn stop(&mut self) -> Result<(), String> {
        #[cfg(unix)]
        let term = signal_group(self.1, libc::SIGTERM);
        #[cfg(windows)]
        let term = if self.0.try_wait().map_err(|e| e.to_string())?.is_none() {
            let mut killer = tokio::process::Command::new("taskkill");
            killer
                .args(["/PID", &self.1.to_string(), "/T", "/F"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            match tokio::time::timeout(Duration::from_secs(5), killer.status()).await {
                Ok(Ok(status)) if status.success() => Ok(()),
                _ => Err("owned process-tree cleanup failed".to_owned()),
            }
        } else {
            Ok(())
        };
        let waited = match tokio::time::timeout(Duration::from_secs(3), self.0.wait()).await {
            Ok(result) => result.map(|_| ()).map_err(|e| e.to_string()),
            Err(_) => self.0.kill().await.map_err(|e| e.to_string()),
        };
        #[cfg(unix)]
        let descendants = signal_group(self.1, libc::SIGKILL);
        #[cfg(windows)]
        let descendants: Result<(), String> = Ok(());
        term?;
        waited?;
        descendants
    }
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: i32) -> Result<(), String> {
    // SAFETY: pid is the process group created and captured by OwnedChild::spawn.
    if unsafe { libc::kill(-(pid as i32), signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(format!("owned process-group cleanup failed: {error}"))
    }
}

#[cfg(unix)]
struct Foreground(i32);
#[cfg(unix)]
fn set_foreground(group: i32) -> Result<(), String> {
    // SAFETY: initialized signal sets and valid stack pointers are passed to POSIX APIs.
    unsafe {
        let mut blocked: libc::sigset_t = std::mem::zeroed();
        let mut previous: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut blocked);
        libc::sigaddset(&mut blocked, libc::SIGTTOU);
        let result = libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, &mut previous);
        if result != 0 {
            return Err(std::io::Error::from_raw_os_error(result).to_string());
        }
        let result = libc::tcsetpgrp(libc::STDIN_FILENO, group);
        let error = std::io::Error::last_os_error();
        libc::pthread_sigmask(libc::SIG_SETMASK, &previous, std::ptr::null_mut());
        if result != 0 {
            return Err(format!("set client foreground: {error}"));
        }
    }
    Ok(())
}
#[cfg(unix)]
impl Foreground {
    fn acquire(pid: u32) -> Result<Option<Self>, String> {
        use std::io::IsTerminal;
        if !std::io::stdin().is_terminal() {
            return Ok(None);
        }
        // SAFETY: tcgetpgrp reads the foreground group of the inherited terminal.
        let previous = unsafe { libc::tcgetpgrp(libc::STDIN_FILENO) };
        if previous < 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        set_foreground(pid as i32)?;
        // SAFETY: only the newly created client process group is resumed.
        unsafe {
            libc::kill(-(pid as i32), libc::SIGCONT);
        }
        Ok(Some(Self(previous)))
    }
}
#[cfg(unix)]
impl Drop for Foreground {
    fn drop(&mut self) {
        if let Err(error) = set_foreground(self.0) {
            eprintln!("{error}");
        }
    }
}

async fn interrupted() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

pub(crate) async fn run(request: &RunRequest) -> Result<u8, String> {
    let plan = plan(request)?;
    let home = dirs::home_dir().ok_or("home directory unavailable")?;
    let claude = crate::claude::find_claude_bin(&home).ok_or("claude executable not found")?;
    let daemon = crate::router::resolve_rld_bin(&home).map_err(|e| e.to_string())?;
    private_directory(&plan.root)?;
    private_directory(&plan.root.join("claude"))?;
    fs::write(plan.root.join("router.json"), &plan.config).map_err(|e| e.to_string())?;
    fs::write(plan.root.join("settings.json"), b"{}\n").map_err(|e| e.to_string())?;
    fs::write(plan.root.join("mcp.json"), b"{\"mcpServers\":{}}\n").map_err(|e| e.to_string())?;
    let log = fs::File::create(plan.root.join("rld.log")).map_err(|e| e.to_string())?;
    let mut command = daemon_command(&daemon, &plan);
    command
        .stdout(log.try_clone().map_err(|e| e.to_string())?)
        .stderr(log);
    let mut host = OwnedChild::spawn(command)?;
    let result = async {
        let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_millis(500)).build().map_err(|e| e.to_string())?;
        let ready = async {
            for _ in 0..100 {
                if host.0.try_wait().map_err(|e| e.to_string())?.is_some() {
                    return Err("ARC host exited before readiness; inspect private rld.log".to_owned());
                }
                if client.get(format!("http://127.0.0.1:{}/healthz", plan.ports[0])).send().await.is_ok_and(|response| response.status().is_success()) { return Ok(()); }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err("ARC host readiness timed out; inspect private rld.log".to_owned())
        };
        tokio::select! { result = tokio::time::timeout(Duration::from_secs(20), ready) => result.map_err(|_| "ARC host readiness timed out")??, _ = interrupted() => return Err("launch interrupted".into()) }
        eprintln!("ARC session routing ready; fresh profile {}", plan.root.display());
        let mut child = OwnedChild::spawn(client_command(&claude, request, &plan))?;
        #[cfg(unix)]
        let foreground = match Foreground::acquire(child.1) {
            Ok(guard) => guard,
            Err(error) => { child.stop().await?; return Err(error); }
        };
        let outcome = tokio::select! {
            result = child.0.wait() => result.map(|status| status.code().unwrap_or(1).clamp(0, 255) as u8).map_err(|e| e.to_string()),
            _ = host.0.wait() => Err("ARC host stopped during the client session".into()),
            _ = interrupted() => Err("session interrupted".into()),
        };
        #[cfg(unix)]
        drop(foreground);
        let cleanup = child.stop().await;
        match (outcome, cleanup) {
            (Err(error), Err(cleanup)) => Err(format!("{error}; client cleanup: {cleanup}")),
            (_, Err(cleanup)) => Err(cleanup),
            (outcome, Ok(())) => outcome,
        }
    }.await;
    match (result, host.stop().await) {
        (Err(error), Err(cleanup)) => Err(format!("{error}; host cleanup: {cleanup}")),
        (_, Err(cleanup)) => Err(cleanup),
        (result, Ok(())) => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::ffi::OsString;

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "rayline-direct-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            fs::create_dir(&root).unwrap();
            let config = json!({
                "endpoints": [{"id":"worker","protocol":"anthropic_messages","base_url":"http://127.0.0.1:9002","api_key_env":"SYNTHETIC_PROVIDER_KEY"}],
                "arc": {"base_url":"http://127.0.0.1:9001","package_alias":"synthetic","package_sha256":"a".repeat(64),"timeout_ms":1000,
                    "session":{"base_url":"http://127.0.0.1:9003","timeout_ms":1000,"max_response_bytes":1048576},
                    "bindings":{"one":{"target":{"endpoint":"worker","model":"synthetic"},"request_overrides":{"thinking":{"type":"disabled"}}}}}
            });
            fs::write(
                root.join("config.json"),
                serde_json::to_vec(&config).unwrap(),
            )
            .unwrap();
            Self(root)
        }
        fn request(&self) -> RunRequest {
            let args: Vec<OsString> = vec![
                "--config".into(),
                self.0.join("config.json").into(),
                "--via".into(),
                "direct".into(),
                "--fresh-profile".into(),
                self.0.join("profile").into(),
                "--".into(),
                "--print".into(),
                "synthetic prompt".into(),
            ];
            crate::parse_claude_request(args.iter().peekable(), None, None, false).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn direct_commands_preserve_config_and_scope_credentials_to_host() {
        let fixture = Fixture::new();
        let request = fixture.request();
        let plan = plan(&request).unwrap();
        assert_eq!(
            plan.config,
            fs::read(fixture.0.join("config.json")).unwrap()
        );
        assert!(!plan.root.exists());
        let client = client_command(Path::new("claude"), &request, &plan);
        let env: std::collections::HashMap<_, _> = client
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert_eq!(env["ANTHROPIC_MODEL"].as_deref(), Some("rayline-arc"));
        assert_eq!(env["SYNTHETIC_PROVIDER_KEY"], None);
        assert_eq!(env["CLAUDE_CODE_DISABLE_AGENT_VIEW"].as_deref(), Some("1"));
        assert!(!env.contains_key("HOME"));
        assert_eq!(
            env["CLAUDE_CONFIG_DIR"].as_deref(),
            plan.root.join("claude").to_str()
        );
        let args: Vec<_> = client
            .get_args()
            .map(|v| v.to_string_lossy().into_owned())
            .collect();
        assert!(args.windows(2).any(|v| v == ["--setting-sources", ""]));
        assert!(args.ends_with(&["--print".into(), "synthetic prompt".into()]));
        let daemon = daemon_command(Path::new("rld"), &plan);
        let args: Vec<_> = daemon
            .get_args()
            .map(|v| v.to_string_lossy().into_owned())
            .collect();
        assert!(args.contains(&"--no-local-model".into()));
        assert!(!args.contains(&"--proxy-port".into()));
        assert!(args.windows(2).any(|v| v[0] == "--router-config-path"
            && v[1] == plan.root.join("router.json").to_string_lossy()));
    }

    #[test]
    fn direct_custom_model_metadata_names_the_route_without_changing_model_identity() {
        let fixture = Fixture::new();
        let request = fixture.request();
        let plan = plan(&request).unwrap();
        let client = client_command(Path::new("claude"), &request, &plan);
        for (key, expected) in [
            ("ANTHROPIC_CUSTOM_MODEL_OPTION", "rayline-arc"),
            (
                "ANTHROPIC_CUSTOM_MODEL_OPTION_NAME",
                "Rayline ARC — model routing",
            ),
            (
                "ANTHROPIC_CUSTOM_MODEL_OPTION_DESCRIPTION",
                "ARC selects the configured model endpoint for each turn.",
            ),
            ("ANTHROPIC_MODEL", "rayline-arc"),
            ("ANTHROPIC_DEFAULT_OPUS_MODEL", "rayline-arc"),
            ("ANTHROPIC_DEFAULT_SONNET_MODEL", "rayline-arc"),
            ("ANTHROPIC_DEFAULT_HAIKU_MODEL", "rayline-arc"),
        ] {
            assert_eq!(
                client.get_envs().find(|(name, _)| *name == key).unwrap().1,
                Some(std::ffi::OsStr::new(expected)),
                "{key}"
            );
        }
        let args: Vec<_> = client.get_args().collect();
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "rayline-arc"])
        );
        assert!(!plan.root.exists());
    }

    #[test]
    fn direct_output_cap_reaches_client_only_and_absence_keeps_client_default() {
        let fixture = Fixture::new();
        let request = fixture.request();
        for (input, expected) in [
            (None, None),
            (Some("512"), Some("512")),
            (Some("0001"), Some("1")),
            (Some("18446744073709551615"), Some("18446744073709551615")),
        ] {
            let plan = plan_with_token_limits(&request, input.map(OsString::from), None).unwrap();
            let client = client_command(Path::new("claude"), &request, &plan);
            let value = client
                .get_envs()
                .find(|(key, _)| *key == MAX_OUTPUT_TOKENS_ENV)
                .unwrap()
                .1;
            assert_eq!(value, expected.map(std::ffi::OsStr::new));
            let daemon = daemon_command_with_env(Path::new("rld"), &plan, |_| None);
            assert!(
                !daemon
                    .get_envs()
                    .any(|(key, value)| key == MAX_OUTPUT_TOKENS_ENV && value.is_some())
            );
            assert!(!plan.root.exists());
        }
    }

    #[test]
    fn invalid_direct_output_cap_fails_before_profile_creation() {
        let fixture = Fixture::new();
        let request = fixture.request();
        for value in [
            "",
            "0",
            "-1",
            "1.5",
            "512x",
            " 512",
            "512 ",
            "18446744073709551616",
        ] {
            let error = plan_with_token_limits(&request, Some(value.into()), None)
                .err()
                .unwrap();
            assert_eq!(
                error,
                "CLAUDE_CODE_MAX_OUTPUT_TOKENS must be a positive decimal u64 integer"
            );
            assert!(!fixture.0.join("profile").exists());
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            assert!(
                plan_with_token_limits(&request, Some(OsString::from_vec(vec![0xff])), None)
                    .is_err()
            );
        }
    }

    #[test]
    fn direct_context_budget_reaches_client_only_and_absence_keeps_default() {
        let fixture = Fixture::new();
        let request = fixture.request();
        for (input, expected) in [
            (None, None),
            (Some("16384"), Some("16384")),
            (Some("0001"), Some("1")),
            (Some("18446744073709551615"), Some("18446744073709551615")),
        ] {
            let plan =
                plan_with_token_limits(&request, Some("512".into()), input.map(OsString::from))
                    .unwrap();
            let client = client_command(Path::new("claude"), &request, &plan);
            let value = client
                .get_envs()
                .find(|(key, _)| *key == MAX_CONTEXT_TOKENS_ENV)
                .unwrap()
                .1;
            assert_eq!(value, expected.map(std::ffi::OsStr::new));
            assert_eq!(
                client
                    .get_envs()
                    .find(|(key, _)| *key == MAX_OUTPUT_TOKENS_ENV)
                    .unwrap()
                    .1,
                Some(std::ffi::OsStr::new("512"))
            );
            let daemon = daemon_command_with_env(Path::new("rld"), &plan, |_| None);
            assert!(
                !daemon
                    .get_envs()
                    .any(|(key, value)| key == MAX_CONTEXT_TOKENS_ENV && value.is_some())
            );
            assert!(!plan.root.exists());
        }
    }

    #[test]
    fn invalid_context_budget_refuses_before_profile_or_config_access() {
        let fixture = Fixture::new();
        let mut request = fixture.request();
        request.config_path = Some(fixture.0.join("nonexistent-config.json"));
        for value in [
            "",
            "0",
            "-1",
            "1.5",
            "16384x",
            " 16384",
            "16384 ",
            "18446744073709551616",
        ] {
            let error = plan_with_token_limits(&request, None, Some(value.into()))
                .err()
                .unwrap();
            assert_eq!(
                error,
                "CLAUDE_CODE_MAX_CONTEXT_TOKENS must be a positive decimal u64 integer"
            );
            assert!(!fixture.0.join("profile").exists());
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            assert!(
                plan_with_token_limits(&request, None, Some(OsString::from_vec(vec![0xff])))
                    .is_err()
            );
        }
    }

    #[test]
    fn configured_native_provider_key_reaches_only_daemon() {
        let fixture = Fixture::new();
        let request = fixture.request();
        let mut plan = plan(&request).unwrap();
        plan.provider_keys = vec!["ANTHROPIC_API_KEY".into()];
        let daemon = daemon_command_with_env(Path::new("rld"), &plan, |name| {
            assert_eq!(name, "ANTHROPIC_API_KEY");
            Some("synthetic-provider-secret".into())
        });
        let value = daemon
            .get_envs()
            .find(|(name, _)| *name == "ANTHROPIC_API_KEY")
            .unwrap()
            .1;
        assert_eq!(
            value,
            Some(std::ffi::OsStr::new("synthetic-provider-secret"))
        );
        let client = client_command(Path::new("claude"), &request, &plan);
        let value = client
            .get_envs()
            .find(|(name, _)| *name == "ANTHROPIC_API_KEY")
            .unwrap()
            .1;
        assert_eq!(value, Some(std::ffi::OsStr::new("rayline-local")));
        assert!(
            !client
                .get_envs()
                .any(|(_, value)| value == Some(std::ffi::OsStr::new("synthetic-provider-secret")))
        );
    }

    #[test]
    fn fresh_profile_refuses_reuse_replay_and_client_overrides_before_writes() {
        let fixture = Fixture::new();
        let mut request = fixture.request();
        request.model = Some("other".into());
        assert!(plan(&request).is_err());
        request.model = None;
        request.args = vec!["--settings=shared.json".into()];
        assert!(plan(&request).is_err());
        request.args.clear();
        fs::create_dir(fixture.0.join("profile")).unwrap();
        assert!(plan(&request).is_err());
        fs::remove_dir(fixture.0.join("profile")).unwrap();
        let file = fixture.0.join("config.json");
        let mut config: serde_json::Value =
            serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
        config["arc"].as_object_mut().unwrap().remove("session");
        fs::write(file, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(plan(&request).is_err());
        assert!(!fixture.0.join("profile").exists());
    }

    #[test]
    fn parser_keeps_direct_mode_explicit_and_existing_isolation_unchanged() {
        let parse = |args: &[&str]| {
            let args: Vec<OsString> = args.iter().map(OsString::from).collect();
            crate::parse_claude_request(args.iter().peekable(), None, None, false)
        };
        for args in [
            vec!["--via", "direct"],
            vec![
                "--config",
                "c",
                "--via",
                "direct",
                "--fresh-profile",
                "/tmp/fresh",
                "--isolated",
            ],
            vec![
                "--config",
                "c",
                "--via",
                "direct",
                "--fresh-profile",
                "/tmp/fresh",
                "--route",
                "subagents",
            ],
            vec!["--config", "c", "--via", "env"],
            vec!["--fresh-profile", "/tmp/fresh"],
        ] {
            assert!(parse(&args).is_none(), "{args:?}");
        }
        let previous = parse(&["--local", "--isolated"]).unwrap();
        assert!(previous.isolated);
        assert!(!previous.direct_local);
        assert!(previous.fresh_profile.is_none());
    }

    #[tokio::test]
    async fn arc_proxy_launch_refuses_before_client_resolution_or_side_effects() {
        let fixture = Fixture::new();
        let mut request = fixture.request();
        request.direct_local = false;
        request.fresh_profile = None;
        let error = crate::claude::run_command(&request).await.unwrap_err();
        assert!(error.to_string().contains("--via direct"));
        assert!(!fixture.0.join("profile").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn owned_process_cleanup_handles_exit_and_running_child() {
        for script in ["exit 0", "sleep 30"] {
            let mut command = Command::new("/bin/sh");
            command
                .args(["-c", script])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let mut child = OwnedChild::spawn(command).unwrap();
            if script == "exit 0" {
                child.0.wait().await.unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), child.stop())
                .await
                .unwrap()
                .unwrap();
            assert!(child.0.try_wait().unwrap().is_some());
        }
    }

    #[test]
    fn arc_config_requires_local_router() {
        let fixture = Fixture::new();
        let path = fixture.0.join("config.json");
        assert!(crate::router_config::config_needs_local_router(&path));
    }
}
