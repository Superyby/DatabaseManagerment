//! AI 服务应用状态

use common::config::{AppConfig, ServiceUrls};

/// AI 服务配置
#[derive(Clone)]
pub struct AiConfig {
    /// LLM API 基础 URL
    pub llm_base_url: String,

    /// LLM API Key
    pub llm_api_key: String,

    /// 默认模型（快速模型）
    pub default_model: String,

    /// 高精度模型
    pub high_precision_model: String,

    /// 最大 Token 数
    pub max_tokens: u32,

    /// 置信度阈值（低于此值触发澄清）
    pub confidence_threshold: f64,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            llm_base_url: std::env::var("LLM_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            llm_api_key: std::env::var("LLM_API_KEY").unwrap_or_default(),
            default_model: std::env::var("LLM_DEFAULT_MODEL")
                .unwrap_or_else(|_| "gpt-4o-mini".to_string()),
            high_precision_model: std::env::var("LLM_HIGH_PRECISION_MODEL")
                .unwrap_or_else(|_| "gpt-4o".to_string()),
            max_tokens: std::env::var("LLM_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4000),
            confidence_threshold: std::env::var("LLM_CONFIDENCE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.7),
        }
    }
}

/// 应用状态
#[derive(Clone)]
pub struct AppState {
    /// 通用配置
    pub config: AppConfig,

    /// AI 配置
    pub ai_config: AiConfig,

    /// 服务 URL 配置
    pub service_urls: ServiceUrls,

    /// HTTP 客户端
    pub http_client: reqwest::Client,
}

impl AppState {
    /// 创建新的应用状态
    pub fn new(config: AppConfig) -> Self {
        Self {
            config,
            ai_config: AiConfig::default(),
            service_urls: ServiceUrls::load(),
            http_client: reqwest::Client::new(),
        }
    }
}
