//! AI 查询服务模块

use tracing::{info, warn};
use uuid::Uuid;

use common::config::ServiceUrls;
use common::errors::{AppError, AppResult};
use common::utils::SqlValidator;

use crate::models::{
    ClarifyRequest, ClarifyResponse, LineageSummary, NaturalQueryRequest, NaturalQueryResponse,
    QueryStatus, SqlReference, ValidateSqlRequest, ValidateSqlResponse, ValidationError,
};
use crate::state::AiConfig;

/// AI 查询服务
pub struct AiQueryService {
    ai_config: AiConfig,
    service_urls: ServiceUrls,
    http_client: reqwest::Client,
}

impl AiQueryService {
    /// 创建新的 AI 查询服务实例
    pub fn new(
        ai_config: AiConfig,
        service_urls: ServiceUrls,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            ai_config,
            service_urls,
            http_client,
        }
    }

    /// 处理自然语言查询
    pub async fn process_natural_query(
        &self,
        req: NaturalQueryRequest,
    ) -> AppResult<NaturalQueryResponse> {
        let trace_id = Uuid::new_v4().to_string();

        info!(
            request_id = %req.request_id,
            trace_id = %trace_id,
            question = %req.question,
            "处理自然语言查询"
        );

        // 1. 获取 Schema 信息
        let schema_info = self.get_schema_info(&req.connection_id).await?;

        // 2. TODO: RAG 检索相关上下文
        // let rag_context = self.search_rag_context(&req.question).await?;

        // 3. TODO: 调用 LLM 生成 SQL
        // let llm_response = self.call_llm(&req.question, &schema_info, &rag_context).await?;

        // 4. TODO: 解析 LLM 响应，提取 SQL 和置信度
        // 目前返回占位响应

        // 占位实现 - 演示响应结构
        let response = NaturalQueryResponse {
            request_id: req.request_id,
            trace_id,
            status: QueryStatus::Ready,
            sql: Some("SELECT * FROM example LIMIT 10".to_string()),
            explanation: Some("这是一个示例查询，返回 example 表的前 10 条记录。".to_string()),
            confidence: Some(0.85),
            references: vec![SqlReference {
                ref_type: "example".to_string(),
                id: "demo_001".to_string(),
                description: Some("示例查询".to_string()),
            }],
            clarification: None,
            lineage_summary: Some(LineageSummary {
                source_tables: vec!["example".to_string()],
                key_columns: vec![],
                applied_rules: vec![],
            }),
        };

        Ok(response)
    }

    /// 处理澄清回复
    pub async fn process_clarification(&self, req: ClarifyRequest) -> AppResult<ClarifyResponse> {
        let trace_id = Uuid::new_v4().to_string();

        info!(
            request_id = %req.request_id,
            original_request_id = %req.original_request_id,
            question_id = %req.question_id,
            answer = %req.answer,
            "处理澄清回复"
        );

        // TODO: 实现澄清逻辑
        // 1. 获取原始请求上下文
        // 2. 结合澄清答案重新生成 SQL

        // 占位实现
        let response = ClarifyResponse {
            request_id: req.request_id,
            trace_id,
            status: QueryStatus::Ready,
            sql: Some("SELECT * FROM example WHERE created_at >= '2024-01-01' LIMIT 10".to_string()),
            explanation: Some("根据您的澄清，查询 2024 年以来的数据。".to_string()),
            confidence: Some(0.90),
            references: vec![],
            clarification: None,
            lineage_summary: None,
        };

        Ok(response)
    }

    /// 校验 SQL
    pub async fn validate_sql(&self, req: ValidateSqlRequest) -> AppResult<ValidateSqlResponse> {
        info!(
            sql_length = req.sql.len(),
            connection_id = %req.connection_id,
            run_explain = req.run_explain,
            "校验 SQL"
        );

        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // 1. 基础语法校验
        if let Err(e) = SqlValidator::validate(&req.sql) {
            errors.push(ValidationError {
                code: "SQL_INVALID".to_string(),
                message: e.to_string(),
            });
        }

        // 2. 检查是否为只读查询
        let sql_upper = req.sql.to_uppercase();
        let dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE"];

        for keyword in dangerous_keywords {
            if sql_upper.contains(keyword) {
                errors.push(ValidationError {
                    code: "WRITE_OPERATION".to_string(),
                    message: format!("不允许执行 {} 操作", keyword),
                });
            }
        }

        // 3. 检查是否有 LIMIT
        if !sql_upper.contains("LIMIT") {
            warnings.push("建议添加 LIMIT 限制返回行数".to_string());
        }

        // 4. TODO: 执行 EXPLAIN 预检
        let explain_summary = if req.run_explain && errors.is_empty() {
            // 占位实现
            None
        } else {
            None
        };

        // 5. 评估风险等级
        let risk_level = if !errors.is_empty() {
            Some("high".to_string())
        } else if !warnings.is_empty() {
            Some("medium".to_string())
        } else {
            Some("low".to_string())
        };

        Ok(ValidateSqlResponse {
            valid: errors.is_empty(),
            errors,
            warnings,
            risk_level,
            explain_summary,
        })
    }

    /// 获取数据库 Schema 信息
    async fn get_schema_info(&self, connection_id: &str) -> AppResult<serde_json::Value> {
        // TODO: 从 connection-service 获取 Schema 信息
        // 目前返回空对象作为占位

        info!(connection_id = %connection_id, "获取 Schema 信息");

        Ok(serde_json::json!({
            "connection_id": connection_id,
            "tables": []
        }))
    }

    /// 调用 LLM 生成 SQL
    #[allow(dead_code)]
    async fn call_llm(
        &self,
        question: &str,
        schema_info: &serde_json::Value,
        _rag_context: &str,
    ) -> AppResult<LlmResponse> {
        // 检查 API Key 是否配置
        if self.ai_config.llm_api_key.is_empty() {
            warn!("LLM API Key 未配置");
            return Err(AppError::Configuration("LLM API Key 未配置".to_string()));
        }

        // TODO: 实现 LLM 调用
        // 1. 构建 Prompt
        // 2. 调用 LLM API
        // 3. 解析响应

        info!(
            question = %question,
            model = %self.ai_config.default_model,
            "调用 LLM"
        );

        // 占位实现
        Ok(LlmResponse {
            sql: "SELECT * FROM example LIMIT 10".to_string(),
            explanation: "示例查询".to_string(),
            confidence: 0.85,
        })
    }
}

/// LLM 响应结构（内部使用）
#[allow(dead_code)]
struct LlmResponse {
    sql: String,
    explanation: String,
    confidence: f64,
}
