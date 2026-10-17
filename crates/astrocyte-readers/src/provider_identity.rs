// Copied from tokscale v4.17.0,
// crates/tokscale-core/src/provider_identity.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../LICENSE-tokscale for the full upstream license text.
// ASTROCYTE DIVERGENCE: this copy retains only the provider inference surface
// required by the Pi reader and the upstream tests that exercise that surface.

fn contains_delimited(haystack: &str, needle: &str) -> bool {
    for (pos, _) in haystack.match_indices(needle) {
        let before_ok = pos == 0 || !haystack.as_bytes()[pos - 1].is_ascii_alphanumeric();
        let after_pos = pos + needle.len();
        let after_ok =
            after_pos == haystack.len() || !haystack.as_bytes()[after_pos].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
    }
    false
}

/// Match a model-family token at a real leading boundary. A decimal digit may
/// immediately follow the family because catalog ids commonly concatenate a
/// major version (`gpt4`, `claude3`, `qwen3`); an ASCII letter may not, which
/// rejects ordinary words that merely contain a family name.
fn contains_versioned_family(haystack: &str, family: &str) -> bool {
    for (pos, _) in haystack.match_indices(family) {
        let before_ok = haystack[..pos]
            .chars()
            .next_back()
            .is_none_or(|character| !character.is_alphanumeric());
        let after_pos = pos + family.len();
        let after_ok = haystack[after_pos..]
            .chars()
            .next()
            .is_none_or(|character| character.is_ascii_digit() || !character.is_alphanumeric());
        if before_ok && after_ok {
            return true;
        }
    }
    false
}

fn contains_family(haystack: &str, family: &str, delimiter_aware: bool) -> bool {
    if delimiter_aware {
        contains_versioned_family(haystack, family)
    } else {
        haystack.contains(family)
    }
}

pub fn inferred_provider_from_model(model: &str) -> Option<&'static str> {
    inferred_provider_from_model_inner(model, false)
}

// ASTROCYTE DIVERGENCE: only copied inference tests use boundary-delimited
// family matching; production inference keeps upstream's normal matching mode.
#[cfg(test)]
fn inferred_provider_from_model_delimited(model: &str) -> Option<&'static str> {
    inferred_provider_from_model_inner(model, true)
}

fn inferred_provider_from_model_inner(
    model: &str,
    delimit_family_names: bool,
) -> Option<&'static str> {
    let lower = model.to_lowercase();

    // Ollama is a routing prefix, not part of the upstream model family. In
    // particular, matching the `llama` in `ollama/...` would label every
    // otherwise-unknown Ollama model as Meta. Re-run inference on the routed
    // model so known families retain their actual providers.
    if let Some(routed_model) = lower.strip_prefix("ollama/") {
        return inferred_provider_from_model_inner(routed_model, delimit_family_names);
    }

    if contains_family(&lower, "claude", delimit_family_names)
        || contains_family(&lower, "anthropic", delimit_family_names)
        || contains_versioned_family(&lower, "opus")
        || contains_versioned_family(&lower, "sonnet")
        || contains_versioned_family(&lower, "haiku")
        || contains_versioned_family(&lower, "fable")
    {
        return Some("anthropic");
    }

    if contains_family(&lower, "gpt", delimit_family_names)
        || contains_family(&lower, "openai", delimit_family_names)
        || contains_delimited(&lower, "o1")
        || contains_delimited(&lower, "o3")
        || contains_delimited(&lower, "o4")
    {
        return Some("openai");
    }

    if contains_family(&lower, "gemini", delimit_family_names)
        || contains_family(&lower, "google", delimit_family_names)
    {
        return Some("google");
    }
    if contains_family(&lower, "grok", delimit_family_names) {
        return Some("xai");
    }
    if contains_family(&lower, "deepseek", delimit_family_names) {
        return Some("deepseek");
    }
    if contains_family(&lower, "minimax", delimit_family_names) {
        return Some("minimax");
    }
    if contains_family(&lower, "mistral", delimit_family_names)
        || contains_family(&lower, "mixtral", delimit_family_names)
    {
        return Some("mistral");
    }
    if contains_family(&lower, "llama", delimit_family_names)
        || contains_versioned_family(&lower, "meta")
    {
        return Some("meta");
    }
    if contains_family(&lower, "qwen", delimit_family_names) {
        return Some("qwen");
    }

    // Sakana's `fugu` / `fugu-ultra` model line. Bare `fugu` is intentionally
    // still mapped to the sakana provider here (provider identity is independent
    // of whether tokscale can price the model).
    if contains_family(&lower, "fugu", delimit_family_names) {
        return Some("sakana");
    }

    // Kimi (Moonshot AI) — `kimi`, `kimi-k2.5`, `kimi-code` variants.
    if contains_versioned_family(&lower, "kimi") {
        return Some("moonshotai");
    }
    // Kimi's coding-plan catalog also serves bare `k2`/`k3`-style ids.
    if contains_delimited(&lower, "k2") || contains_delimited(&lower, "k3") {
        return Some("moonshotai");
    }
    if contains_versioned_family(&lower, "mimo") {
        return Some("xiaomi");
    }
    if contains_versioned_family(&lower, "glm") {
        return Some("zai");
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fable_models_map_to_anthropic() {
        assert_eq!(inferred_provider_from_model("fable-5"), Some("anthropic"));
        assert_eq!(
            inferred_provider_from_model("claude-fable-5"),
            Some("anthropic")
        );
        assert_eq!(
            inferred_provider_from_model("claude-fable-5[1m]"),
            Some("anthropic")
        );
    }

    #[test]
    fn test_inferred_provider_from_model() {
        assert_eq!(
            inferred_provider_from_model("claude-sonnet-4"),
            Some("anthropic")
        );
        assert_eq!(inferred_provider_from_model("gpt-5.2"), Some("openai"));
        assert_eq!(
            inferred_provider_from_model("gemini-2.5-pro"),
            Some("google")
        );
        assert_eq!(
            inferred_provider_from_model("grok-code-fast-1"),
            Some("xai")
        );
        assert_eq!(
            inferred_provider_from_model("deepseek-v3"),
            Some("deepseek")
        );
        assert_eq!(
            inferred_provider_from_model("MiniMax-M2.1"),
            Some("minimax")
        );
        assert_eq!(
            inferred_provider_from_model("mixtral-8x7b"),
            Some("mistral")
        );
        assert_eq!(inferred_provider_from_model("llama-3"), Some("meta"));
        assert_eq!(inferred_provider_from_model("qwen3-coder"), Some("qwen"));
        assert_eq!(inferred_provider_from_model("unknown-model"), None);
    }

    #[test]
    fn test_inferred_provider_bare_kimi_k_series_ids() {
        assert_eq!(inferred_provider_from_model("k3"), Some("moonshotai"));
        assert_eq!(inferred_provider_from_model("k3-256k"), Some("moonshotai"));
        assert_eq!(inferred_provider_from_model("K3"), Some("moonshotai"));
        assert_eq!(inferred_provider_from_model("k2"), Some("moonshotai"));
        assert_eq!(
            inferred_provider_from_model("kimi-k2.5-thinking"),
            Some("moonshotai")
        );
        assert_eq!(inferred_provider_from_model("flock3"), None);
        assert_eq!(inferred_provider_from_model("network2"), None);
    }

    #[test]
    fn test_inferred_provider_ignores_ollama_route_prefix() {
        assert_eq!(inferred_provider_from_model("ollama/orca-mini"), None);
        assert_eq!(
            inferred_provider_from_model("ollama/qwen3-coder"),
            Some("qwen")
        );
        assert_eq!(
            inferred_provider_from_model("ollama/llama-3.3"),
            Some("meta")
        );
    }

    #[test]
    fn test_inferred_provider_fugu_maps_to_sakana() {
        assert_eq!(inferred_provider_from_model("fugu"), Some("sakana"));
        assert_eq!(inferred_provider_from_model("fugu-ultra"), Some("sakana"));
        assert_eq!(inferred_provider_from_model("FUGU"), Some("sakana"));
    }

    #[test]
    fn test_inferred_provider_no_false_positives() {
        for model in [
            "protocol1-fast",
            "proto3-server",
            "co4pilot-v2",
            "metadata-model",
            "metamorphic-v1",
        ] {
            assert_eq!(inferred_provider_from_model(model), None, "{model}");
        }
    }

    #[test]
    fn delimiter_aware_inference_accepts_family_versions_not_word_substrings() {
        let genuine = [
            ("gpt4-turbo", "openai"),
            ("claude3-opus", "anthropic"),
            ("opus4", "anthropic"),
            ("gemini2-pro", "google"),
            ("grok3", "xai"),
            ("deepseek3", "deepseek"),
            ("minimax2", "minimax"),
            ("mistral4", "mistral"),
            ("llama3", "meta"),
            ("qwen3-coder", "qwen"),
            ("fugu2", "sakana"),
            ("kimi2", "moonshotai"),
            ("mimo2", "xiaomi"),
            ("glm5", "zai"),
        ];
        for (model, provider) in genuine {
            assert_eq!(
                inferred_provider_from_model_delimited(model),
                Some(provider),
                "{model}"
            );
        }

        for model in [
            "agpt-model",
            "declaude-x",
            "pregeminified",
            "engroked",
            "deepseeking",
            "minimaximal",
            "demistralized",
            "remixtralized",
            "collamated",
            "unqwened-model",
            "defugued",
            "skimimoed",
            "glimmer",
            "unkimied",
        ] {
            assert_eq!(
                inferred_provider_from_model_delimited(model),
                None,
                "{model} is not a genuine family token"
            );
        }
    }

    #[test]
    fn test_inferred_provider_matches_version_suffixed_and_embedded_families() {
        for model in [
            "qwen3-coder",
            "qwen3.7-plus",
            "qwen2-5-14b-instruct",
            "qwen3-235b-a22b-instruct-2507",
        ] {
            assert_eq!(inferred_provider_from_model(model), Some("qwen"), "{model}");
        }
        assert_eq!(
            inferred_provider_from_model("chatgpt-4o-latest"),
            Some("openai")
        );
        assert_eq!(
            inferred_provider_from_model("CodeLlama-34b-Instruct-hf"),
            Some("meta")
        );
    }

    #[test]
    fn test_inferred_provider_boundary_matches() {
        assert_eq!(inferred_provider_from_model("o1-preview"), Some("openai"));
        assert_eq!(inferred_provider_from_model("o3-mini"), Some("openai"));
        assert_eq!(inferred_provider_from_model("o4-mini"), Some("openai"));
        assert_eq!(inferred_provider_from_model("meta-llama-3"), Some("meta"));
    }
}
