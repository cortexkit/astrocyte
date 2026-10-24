//! Harness-owned layouts, deliberately separate from Astrocyte's own directories.

use std::path::PathBuf;

use cortexkit_paths::{env_absolute_path, with_env, EnvSource, PathsError};

pub(crate) struct ClaudeWorkerPaths {
    pub(crate) roots: PathBuf,
    pub(crate) ack: PathBuf,
}

/// Accept the same resolved data home as Astrocyte's store, never a cwd-relative one.
pub(crate) fn claude_workers(data_home: PathBuf) -> Result<ClaudeWorkerPaths, String> {
    if !data_home.is_absolute() {
        return Err(format!("the data home resolved to the relative {data_home:?}; refusing to resolve Claude worker roots"));
    }
    let cortexkit = data_home.join("cortexkit");
    Ok(ClaudeWorkerPaths {
        roots: cortexkit.join("thalamus-harness/claude-workers/roots.json"),
        ack: cortexkit.join("astrocyte/acks/claude-workers.json"),
    })
}

/// The roots file declaring the OpenCode sessions thalamus-harness runs in
/// private OpenCode servers. The session folders sit beside it. Like the
/// Claude worker roots, it is resolved under Astrocyte's own data home.
pub(crate) fn opencode_sessions_roots(data_home: PathBuf) -> Result<PathBuf, String> {
    if !data_home.is_absolute() {
        return Err(format!("the data home resolved to the relative {data_home:?}; refusing to resolve OpenCode session roots"));
    }
    Ok(data_home.join("cortexkit/thalamus-harness/opencode-sessions/roots.json"))
}

// OpenCode keeps one database: `OPENCODE_DB` when set (a file path, not a
// directory), else `<XDG data home>/opencode/opencode.db`. Its data home comes
// from the xdg-basedir package, which uses XDG_DATA_HOME or
// `<home>/.local/share` on every OS, Windows included, so this deliberately
// does not use the Windows application-data folders. OpenCode itself accepts a
// relative OPENCODE_DB; a meter that guessed what it is relative to would read
// the wrong file, so a relative value is refused. Sources: OpenCode v1.3.0
// packages/opencode/src/global/index.ts:2-9 and storage/db.ts:30-39
// (https://github.com/anomalyco/opencode/tree/v1.3.0); xdg-basedir
// index.js:4-8 (https://github.com/sindresorhus/xdg-basedir/blob/main/index.js#L4-L8).
pub(crate) fn opencode(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    if let Some(path) = env_absolute_path(env, "OPENCODE_DB")? {
        return Ok(path);
    }
    // Reject relative overrides before the shared XDG resolver can fall back.
    env_absolute_path(env, "XDG_DATA_HOME")?;
    Ok(with_env(env)
        .xdg_data_home()?
        .join("opencode")
        .join("opencode.db"))
}

// Pi writes transcripts to `PI_CODING_AGENT_SESSION_DIR` when set; otherwise to
// `sessions` inside its agent directory, which is `PI_CODING_AGENT_DIR` or
// `<home>/.pi/agent`. The session variable names the transcript directory
// itself, so nothing is appended to it. Sources: config.ts:584-610,648-650 and
// main.ts:690-694 in
// https://github.com/badlogic/pi-mono/tree/main/packages/coding-agent/src.
pub(crate) fn pi(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    if let Some(path) = env_absolute_path(env, "PI_CODING_AGENT_SESSION_DIR")? {
        return Ok(path);
    }
    let agent = match env_absolute_path(env, "PI_CODING_AGENT_DIR")? {
        Some(path) => path,
        None => with_env(env).home()?.join(".pi").join("agent"),
    };
    Ok(agent.join("sessions"))
}

// Claude Code keeps per-project transcripts in `projects` inside its config
// directory, which is `CLAUDE_CONFIG_DIR` or `<home>/.claude`; setting the
// variable moves the transcripts with it. Its implementation is not public, so
// this follows its documentation:
// https://code.claude.com/docs/en/settings#settings-files and
// https://code.claude.com/docs/en/how-claude-code-works#work-with-sessions.
pub(crate) fn claude_code(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    let config = match env_absolute_path(env, "CLAUDE_CONFIG_DIR")? {
        Some(path) => path,
        None => with_env(env).home()?.join(".claude"),
    };
    Ok(config.join("projects"))
}

// Codex's home is `CODEX_HOME` or `<home>/.codex`. It writes live transcripts to
// `sessions` and later moves them to `archived_sessions`, both inside that home,
// so this returns the home and the reader reads both; returning `sessions` alone
// would lose every archived call. Sources: utils/home-dir/src/lib.rs:5-17,52-60
// and rollout/src/lib.rs:86-87 in https://github.com/openai/codex/tree/main/codex-rs.
pub(crate) fn codex(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    match env_absolute_path(env, "CODEX_HOME")? {
        Some(path) => Ok(path),
        None => Ok(with_env(env).home()?.join(".codex")),
    }
}

// Gemini's override replaces the home, not the .gemini directory:
// https://github.com/google-gemini/gemini-cli/blob/main/packages/core/src/utils/paths.ts#L13-L28
// and config/storage.ts:54-60 in the same package. Appending .gemini is essential
// for finding transcripts when GEMINI_CLI_HOME is set.
pub(crate) fn gemini(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    let home = match env_absolute_path(env, "GEMINI_CLI_HOME")? {
        Some(path) => path,
        None => with_env(env).home()?,
    };
    Ok(home.join(".gemini").join("tmp"))
}

// Goose's sessions database is `<data>/sessions/sessions.db`, where `<data>` is
// `GOOSE_PATH_ROOT/data` when set; otherwise `<XDG data home>/goose` on macOS
// and Linux, and `%APPDATA%\Block\goose\data` on Windows. Goose resolves this
// with the etcetera crate's app strategy, which is XDG on every non-Windows OS,
// macOS included. Goose's own comments and docs also mention
// `~/Library/Application Support`; that is etcetera's native strategy, which
// Goose never calls, so reading it as well could count a moved store twice.
// Sources: config/paths.rs:9-32 (etcetera 0.11.0)
// https://github.com/block/goose/blob/main/crates/goose/src/config/paths.rs#L9-L32
// and https://block.github.io/goose/docs/guides/logs/.
pub(crate) fn goose(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    let data = if let Some(root) = env_absolute_path(env, "GOOSE_PATH_ROOT")? {
        root.join("data")
    } else {
        #[cfg(windows)]
        {
            env_absolute_path(env, "APPDATA")?
                .ok_or(PathsError::Unresolved {
                    directory: "Goose roaming data directory",
                    variables: &["APPDATA"],
                })?
                .join("Block")
                .join("goose")
                .join("data")
        }
        #[cfg(not(windows))]
        {
            // Reject relative overrides before the shared XDG resolver can fall back.
            env_absolute_path(env, "XDG_DATA_HOME")?;
            with_env(env).xdg_data_home()?.join("goose")
        }
    };
    Ok(data.join("sessions").join("sessions.db"))
}

// Oh My Pi writes transcripts to `<home>/.omp/agent/sessions`. It also reads
// Pi's `PI_CODING_AGENT_DIR` and `PI_CODING_AGENT_SESSION_DIR`, but when those
// are set they point at Pi's own transcripts, which the Pi reader already
// ingests; following them here would record every one of those calls twice, so
// this keeps the default only. Source:
// https://github.com/can1357/oh-my-pi/blob/main/packages/utils/src/dirs.ts#L966-L969
pub(crate) fn omp(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    Ok(with_env(env)
        .home()?
        .join(".omp")
        .join("agent")
        .join("sessions"))
}

// Kimi: https://github.com/MoonshotAI/kimi-cli/blob/main/src/kimi_cli/share.py#L7-L14
// and metadata.py:34-38. KIMI_SHARE_DIR is the share root, not the sessions root.
pub(crate) fn kimi(env: &impl EnvSource) -> Result<PathBuf, PathsError> {
    let share = match env_absolute_path(env, "KIMI_SHARE_DIR")? {
        Some(path) => path,
        None => with_env(env).home()?.join(".kimi"),
    };
    Ok(share.join("sessions"))
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestEnv(std::collections::BTreeMap<&'static str, std::ffi::OsString>);

#[cfg(test)]
impl TestEnv {
    pub(crate) fn set(mut self, key: &'static str, value: impl Into<std::ffi::OsString>) -> Self {
        self.0.insert(key, value.into());
        self
    }
}

#[cfg(test)]
impl EnvSource for TestEnv {
    fn var_os(&self, key: &str) -> Option<std::ffi::OsString> {
        self.0.get(key).cloned()
    }

    fn path_exists(&self, _path: &std::path::Path) -> bool {
        panic!("harness path resolution must not inspect the filesystem")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn absolute_root() -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\Users\reader")
        } else {
            PathBuf::from("/home/reader")
        }
    }

    fn join(root: &Path, components: &[&str]) -> PathBuf {
        components
            .iter()
            .fold(root.to_path_buf(), |path, part| path.join(part))
    }

    macro_rules! default_tests {
        ($name:ident, $resolver:ident, [$($tail:literal),*]) => {
            #[test]
            fn $name() {
                let home = absolute_root();
                let env = TestEnv::default().set("HOME", home.as_os_str());
                assert_eq!($resolver(&env).unwrap(), join(&home, &[$($tail),*]));
            }
        };
    }

    macro_rules! windows_default_tests {
        ($name:ident, $resolver:ident, [$($tail:literal),*]) => {
            #[cfg(windows)]
            #[test]
            fn $name() {
                let home = absolute_root();
                let env = TestEnv::default().set("USERPROFILE", home.as_os_str());
                assert_eq!($resolver(&env).unwrap(), join(&home, &[$($tail),*]));
            }
        };
    }

    default_tests!(
        opencode_home_default,
        opencode,
        [".local", "share", "opencode", "opencode.db"]
    );
    default_tests!(pi_home_default, pi, [".pi", "agent", "sessions"]);
    default_tests!(
        claude_code_home_default,
        claude_code,
        [".claude", "projects"]
    );
    default_tests!(codex_home_default, codex, [".codex"]);
    default_tests!(gemini_home_default, gemini, [".gemini", "tmp"]);
    default_tests!(omp_home_default, omp, [".omp", "agent", "sessions"]);
    default_tests!(kimi_home_default, kimi, [".kimi", "sessions"]);
    #[cfg(not(windows))]
    default_tests!(
        goose_home_default,
        goose,
        [".local", "share", "goose", "sessions", "sessions.db"]
    );

    windows_default_tests!(
        opencode_windows_default,
        opencode,
        [".local", "share", "opencode", "opencode.db"]
    );
    windows_default_tests!(pi_windows_default, pi, [".pi", "agent", "sessions"]);
    windows_default_tests!(
        claude_code_windows_default,
        claude_code,
        [".claude", "projects"]
    );
    windows_default_tests!(codex_windows_default, codex, [".codex"]);
    windows_default_tests!(gemini_windows_default, gemini, [".gemini", "tmp"]);
    windows_default_tests!(omp_windows_default, omp, [".omp", "agent", "sessions"]);
    windows_default_tests!(kimi_windows_default, kimi, [".kimi", "sessions"]);

    #[cfg(windows)]
    #[test]
    fn goose_windows_default() {
        let home = absolute_root();
        let appdata = home.join("AppData").join("Roaming");
        let env = TestEnv::default()
            .set("USERPROFILE", home.as_os_str())
            .set("APPDATA", appdata.as_os_str());
        assert_eq!(
            goose(&env).unwrap(),
            join(
                &appdata,
                &["Block", "goose", "data", "sessions", "sessions.db"]
            )
        );
        for invalid in [None, Some("relative")] {
            let env = match invalid {
                None => TestEnv::default(),
                Some(value) => TestEnv::default().set("APPDATA", value),
            };
            assert!(goose(&env).unwrap_err().to_string().contains("APPDATA"));
        }
    }

    macro_rules! override_tests {
        ($name:ident, $resolver:ident, $var:literal, [$($tail:literal),*]) => {
            #[test]
            fn $name() {
                let root = absolute_root().join("relocated");
                let configured = TestEnv::default().set($var, root.as_os_str());
                assert_eq!($resolver(&configured).unwrap(), join(&root, &[$($tail),*]));
                let relative = TestEnv::default()
                    .set("HOME", absolute_root().as_os_str())
                    .set("USERPROFILE", absolute_root().as_os_str())
                    .set($var, "relative-store");
                let error = $resolver(&relative).unwrap_err().to_string();
                assert!(error.contains($var) && error.contains("must be absolute"), "{error}");
            }
        };
    }

    override_tests!(opencode_override_is_absolute, opencode, "OPENCODE_DB", []);
    override_tests!(
        pi_session_override_is_absolute,
        pi,
        "PI_CODING_AGENT_SESSION_DIR",
        []
    );
    override_tests!(
        pi_agent_override_is_absolute,
        pi,
        "PI_CODING_AGENT_DIR",
        ["sessions"]
    );
    override_tests!(
        claude_code_override_is_absolute,
        claude_code,
        "CLAUDE_CONFIG_DIR",
        ["projects"]
    );
    override_tests!(codex_override_is_absolute, codex, "CODEX_HOME", []);
    override_tests!(
        gemini_override_is_absolute,
        gemini,
        "GEMINI_CLI_HOME",
        [".gemini", "tmp"]
    );
    override_tests!(
        goose_override_is_absolute,
        goose,
        "GOOSE_PATH_ROOT",
        ["data", "sessions", "sessions.db"]
    );
    override_tests!(
        kimi_override_is_absolute,
        kimi,
        "KIMI_SHARE_DIR",
        ["sessions"]
    );

    #[test]
    fn pi_sessions_override_wins_over_agent_override() {
        let root = absolute_root().join("sessions-root");
        let env = TestEnv::default()
            .set("PI_CODING_AGENT_SESSION_DIR", root.as_os_str())
            .set("PI_CODING_AGENT_DIR", "relative-agent");
        assert_eq!(pi(&env).unwrap(), root);
    }

    #[test]
    fn omp_ignores_pi_overrides_including_relative_values() {
        let home = absolute_root();
        for value in [home.join("pi-relocated"), PathBuf::from("relative-pi")] {
            let env = TestEnv::default()
                .set("HOME", home.as_os_str())
                .set("USERPROFILE", home.as_os_str())
                .set("PI_CODING_AGENT_DIR", value.as_os_str())
                .set("PI_CODING_AGENT_SESSION_DIR", value.as_os_str());
            assert_eq!(
                omp(&env).unwrap(),
                join(&home, &[".omp", "agent", "sessions"])
            );
        }
    }

    #[test]
    fn opencode_honors_xdg_even_when_windows_appdata_is_present() {
        let root = absolute_root();
        let xdg = root.join("xdg");
        let env = TestEnv::default()
            .set("HOME", root.as_os_str())
            .set("USERPROFILE", root.as_os_str())
            .set("XDG_DATA_HOME", xdg.as_os_str())
            .set("LOCALAPPDATA", root.join("local"))
            .set("APPDATA", root.join("roaming"));
        assert_eq!(
            opencode(&env).unwrap(),
            xdg.join("opencode").join("opencode.db")
        );
        assert_eq!(
            opencode(&env.set("OPENCODE_DB", root.join("custom.db"))).unwrap(),
            root.join("custom.db")
        );
    }

    #[test]
    fn opencode_relative_xdg_refuses_instead_of_falling_back() {
        let env = TestEnv::default()
            .set("HOME", absolute_root().as_os_str())
            .set("USERPROFILE", absolute_root().as_os_str())
            .set("XDG_DATA_HOME", "relative-data");
        assert!(opencode(&env)
            .unwrap_err()
            .to_string()
            .contains("XDG_DATA_HOME"));
    }

    #[cfg(not(windows))]
    #[test]
    fn goose_relative_xdg_refuses_instead_of_falling_back() {
        let env = TestEnv::default()
            .set("HOME", absolute_root().as_os_str())
            .set("XDG_DATA_HOME", "relative-data");
        assert!(goose(&env)
            .unwrap_err()
            .to_string()
            .contains("XDG_DATA_HOME"));
    }

    #[cfg(not(windows))]
    #[test]
    fn goose_xdg_is_overridden_by_path_root() {
        let root = absolute_root();
        let xdg = root.join("xdg");
        let env = TestEnv::default().set("XDG_DATA_HOME", xdg.as_os_str());
        assert_eq!(
            goose(&env).unwrap(),
            join(&xdg, &["goose", "sessions", "sessions.db"])
        );
        assert_eq!(
            goose(&env.set("GOOSE_PATH_ROOT", root.as_os_str())).unwrap(),
            join(&root, &["data", "sessions", "sessions.db"])
        );
    }

    #[test]
    fn empty_overrides_fall_back_and_missing_homes_refuse() {
        type Resolver = fn(&TestEnv) -> Result<PathBuf, PathsError>;
        let resolvers: [(Resolver, &[&'static str]); 8] = [
            (opencode, &["OPENCODE_DB"]),
            (pi, &["PI_CODING_AGENT_SESSION_DIR", "PI_CODING_AGENT_DIR"]),
            (claude_code, &["CLAUDE_CONFIG_DIR"]),
            (codex, &["CODEX_HOME"]),
            (gemini, &["GEMINI_CLI_HOME"]),
            (goose, &["GOOSE_PATH_ROOT"]),
            (omp, &[]),
            (kimi, &["KIMI_SHARE_DIR"]),
        ];
        let root = absolute_root();
        for (resolve, variables) in resolvers {
            let env = TestEnv::default()
                .set("HOME", root.as_os_str())
                .set("USERPROFILE", root.as_os_str())
                .set("APPDATA", root.join("AppData").join("Roaming"));
            let expected = resolve(&env).unwrap();
            let empty = variables
                .iter()
                .fold(env, |env, variable| env.set(variable, ""));
            assert_eq!(resolve(&empty).unwrap(), expected);
            let error = resolve(&TestEnv::default()).unwrap_err().to_string();
            assert!(
                error.contains(if cfg!(windows) && variables == ["GOOSE_PATH_ROOT"] {
                    "APPDATA"
                } else {
                    "HOME"
                }),
                "{error}"
            );
        }
    }
}
