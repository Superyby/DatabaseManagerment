//! AI 服务数据模型

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

/// 自然语言查询请求
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
pub struct NaturalQueryRequest {
    /// 请求唯一标识
    #[validate(length(min = 1, max = 64))]
    pub request_id: String,

    /// 用户的自然语言问题
    #[validate(length(min = 1, max = 2000))]
    pub question: String,

    /// 目标数据库连接 ID
    #[validate(length(min = 1, max = 64))]
    pub connection_id: String,

    /// 对话上下文（可选，用于多轮对话）
    pub context: Option<ConversationContext>,

    /// 用户权限列表
    #[serde(default)]
    pub user_permissions: Vec<String>,
}

/// 对话上下文
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConversationContext {
    /// 会话 ID
    pub session_id: String,

    /// 历史对话记录
    #[serde(default)]
    pub history: Vec<ChatMessage>,
}

/// 对话消息
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChatMessage {
    /// 角色：user / assistant
    pub role: String,

    /// 消息内容
    pub content: String,
}

/// 自然语言查询响应
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NaturalQueryResponse {
    /// 请求 ID
    pub request_id: String,

    /// 追踪 ID
    pub trace_id: String,

    /// 响应状态
    pub status: QueryStatus,

    /// 生成的 SQL（当状态为 Ready 时存在）
    pub sql: Option<String>,

    /// SQL 的自然语言解释
    pub explanation: Option<String>,

    /// 置信度评分（0.0 - 1.0）
    pub confidence: Option<f64>,

    /// 引用来源
    #[serde(default)]
    pub references: Vec<SqlReference>,

    /// 需要澄清的问题（当状态为 NeedClarification 时存在）
    pub clarification: Option<ClarificationQuestion>,

    /// 血缘摘要
    pub lineage_summary: Option<LineageSummary>,
}

/// 查询状态
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryStatus {
    /// SQL 已生成，可以执行
    Ready,
    /// 需要用户澄清
    NeedClarification,
    /// 生成失败
    Failed,
}

/// SQL 引用来源
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SqlReference {
    /// 引用类型：metric / faq / history_sql / example
    #[serde(rename = "type")]
    pub ref_type: String,

    /// 引用标识
    pub id: String,

    /// 引用描述
    pub description: Option<String>,
}

/// 澄清问题
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClarificationQuestion {
    /// 问题 ID
    pub question_id: String,

    /// 澄清问题内容
    pub question: String,

    /// 澄清维度：time_range / metric / dimension / filter
    pub dimension: String,

    /// 可选项（如有）
    #[serde(default)]
    pub options: Vec<ClarificationOption>,

    /// 默认值（如有）
    pub default_value: Option<String>,
}

/// 澄清选项
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClarificationOption {
    /// 选项值
    pub value: String,

    /// 选项显示文本
    pub label: String,
}

/// 血缘摘要
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LineageSummary {
    /// 数据源表
    #[serde(default)]
    pub source_tables: Vec<String>,

    /// 关键列
    #[serde(default)]
    pub key_columns: Vec<String>,

    /// 应用的规则
    #[serde(default)]
    pub applied_rules: Vec<String>,
}

/// 澄清回复请求
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
pub struct ClarifyRequest {
    /// 请求 ID
    #[validate(length(min = 1, max = 64))]
    pub request_id: String,

    /// 原始问题 ID
    #[validate(length(min = 1, max = 64))]
    pub original_request_id: String,

    /// 澄清问题 ID
    #[validate(length(min = 1, max = 64))]
    pub question_id: String,

    /// 用户回复
    #[validate(length(min = 1, max = 500))]
    pub answer: String,

    /// 目标数据库连接 ID
    #[validate(length(min = 1, max = 64))]
    pub connection_id: String,
}

/// 澄清回复响应（复用 NaturalQueryResponse）
pub type ClarifyResponse = NaturalQueryResponse;

/// SQL 校验请求
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
pub struct ValidateSqlRequest {
    /// 待校验的 SQL
    #[validate(length(min = 1, max = 10000))]
    pub sql: String,

    /// 目标数据库连接 ID
    #[validate(length(min = 1, max = 64))]
    pub connection_id: String,

    /// 是否执行 EXPLAIN 预检
    #[serde(default)]
    pub run_explain: bool,
}

/// SQL 校验响应
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValidateSqlResponse {
    /// 是否通过校验
    pub valid: bool,

    /// 校验错误信息
    #[serde(default)]
    pub errors: Vec<ValidationError>,

    /// 警告信息
    #[serde(default)]
    pub warnings: Vec<String>,

    /// 风险等级：low / medium / high
    pub risk_level: Option<String>,

    /// EXPLAIN 结果摘要（如果执行了预检）
    pub explain_summary: Option<ExplainSummary>,
}

/// 校验错误
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValidationError {
    /// 错误代码
    pub code: String,

    /// 错误消息
    pub message: String,
}

/// EXPLAIN 结果摘要
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExplainSummary {
    /// 预估扫描行数
    pub estimated_rows: Option<u64>,

    /// 是否全表扫描
    pub full_table_scan: bool,

    /// 使用的索引
    #[serde(default)]
    pub indexes_used: Vec<String>,
}
