//! AI 查询服务模块 - DeepSeek LLM 集成

use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

use common::config::ServiceUrls;
use common::errors::{AppError, AppResult};
use common::models::database::TableSchema;
use common::response::ApiResponse;
use common::utils::SqlValidator;

use crate::models::{
    ClarifyRequest, ClarifyResponse, LineageSummary, NaturalQueryRequest, NaturalQueryResponse,
    QueryStatus, SqlReference, ValidateSqlRequest, ValidateSqlResponse, ValidationError,
    ClarificationQuestion, ClarificationOption,
};
use crate::state::AiConfig;

// ============== DeepSeek API Types ==============

/// OpenAI-compatible chat completion request
#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMsg>,
    temperature: f64,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<ResponseFormat>,
}

#[derive(Debug, Serialize)]
struct ChatMsg {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    format_type: String,
}

/// OpenAI-compatible chat completion response
#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: MessageContent,
}

#[derive(Debug, Deserialize)]
struct MessageContent {
    content: String,
}

/// LLM 解析后的 SQL 响应
#[derive(Debug, Deserialize, Default)]
struct LlmSqlResponse {
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    explanation: Option<String>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    source_tables: Option<Vec<String>>,
    #[serde(default)]
    key_columns: Option<Vec<String>>,
    #[serde(default)]
    need_clarification: Option<bool>,
    #[serde(default)]
    clarification_question: Option<String>,
    #[serde(default)]
    clarification_dimension: Option<String>,
    #[serde(default)]
    clarification_options: Option<Vec<String>>,
}

// ============== AI Query Service ==============

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

    /// 处理自然语言查询 - 核心 Text2SQL 流程
    pub async fn process_natural_query(
        &self,
        req: NaturalQueryRequest,
    ) -> AppResult<NaturalQueryResponse> {
        let trace_id = Uuid::new_v4().to_string();

        info!(
            request_id = %req.request_id,
            trace_id = %trace_id,
            question = %req.question,
            connection_id = %req.connection_id,
            "处理自然语言查询"
        );

        // 1. 检查 API Key 配置
        if self.ai_config.llm_api_key.is_empty() {
            return Err(AppError::Configuration("LLM API Key 未配置，请在 .env 中设置 LLM_API_KEY".to_string()));
        }

        // 2. 获取 Schema 信息（从 connection-service）
        let schema = self.get_schema_info(&req.connection_id).await;
        let schema = match schema {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "获取 Schema 失败，将以无 Schema 模式调用 LLM");
                TableSchema {
                    database: String::new(),
                    db_type: String::new(),
                    tables: vec![],
                }
            }
        };

        // 3. 构建对话消息
        let mut messages = vec![ChatMsg {
            role: "system".to_string(),
            content: self.build_system_prompt(&schema),
        }];

        // 添加历史对话（多轮对话支持）
        if let Some(ctx) = &req.context {
            for msg in &ctx.history {
                messages.push(ChatMsg {
                    role: msg.role.clone(),
                    content: msg.content.clone(),
                });
            }
        }

        // 添加当前用户问题
        messages.push(ChatMsg {
            role: "user".to_string(),
            content: req.question.clone(),
        });

        // 4. 调用 DeepSeek LLM
        let llm_raw = self.call_llm(messages).await?;

        info!(
            trace_id = %trace_id,
            response_len = llm_raw.len(),
            "LLM 返回结果"
        );

        // 5. 解析 LLM 响应
        let parsed = self.parse_llm_response(&llm_raw);

        // 6. 构建响应
        if parsed.need_clarification == Some(true) {
            // 需要澄清
            let question_id = Uuid::new_v4().to_string();
            Ok(NaturalQueryResponse {
                request_id: req.request_id,
                trace_id,
                status: QueryStatus::NeedClarification,
                sql: None,
                explanation: parsed.explanation,
                confidence: None,
                references: vec![],
                clarification: Some(ClarificationQuestion {
                    question_id,
                    question: parsed.clarification_question.unwrap_or_else(|| "请提供更多信息".to_string()),
                    dimension: parsed.clarification_dimension.unwrap_or_else(|| "general".to_string()),
                    options: parsed.clarification_options.unwrap_or_default()
                        .into_iter()
                        .map(|o| ClarificationOption { value: o.clone(), label: o })
                        .collect(),
                    default_value: None,
                }),
                lineage_summary: None,
            })
        } else if let Some(sql) = &parsed.sql {
            // SQL 生成成功
            Ok(NaturalQueryResponse {
                request_id: req.request_id,
                trace_id,
                status: QueryStatus::Ready,
                sql: Some(sql.clone()),
                explanation: parsed.explanation,
                confidence: parsed.confidence,
                references: vec![SqlReference {
                    ref_type: "text2sql".to_string(),
                    id: "deepseek".to_string(),
                    description: Some(format!("由 {} 模型生成", self.ai_config.default_model)),
                }],
                clarification: None,
                lineage_summary: Some(LineageSummary {
                    source_tables: parsed.source_tables.unwrap_or_default(),
                    key_columns: parsed.key_columns.unwrap_or_default(),
                    applied_rules: vec![],
                }),
            })
        } else {
            // 生成失败
            Ok(NaturalQueryResponse {
                request_id: req.request_id,
                trace_id,
                status: QueryStatus::Failed,
                sql: None,
                explanation: Some(parsed.explanation.unwrap_or_else(|| "无法根据当前信息生成 SQL 查询".to_string())),
                confidence: Some(0.0),
                references: vec![],
                clarification: None,
                lineage_summary: None,
            })
        }
    }

    /// 处理澄清回复 - 基于用户回答重新生成 SQL
    pub async fn process_clarification(&self, req: ClarifyRequest) -> AppResult<ClarifyResponse> {
        let trace_id = Uuid::new_v4().to_string();

        info!(
            request_id = %req.request_id,
            original_request_id = %req.original_request_id,
            question_id = %req.question_id,
            answer = %req.answer,
            "处理澄清回复"
        );

        // 获取 Schema
        let schema = self.get_schema_info(&req.connection_id).await.unwrap_or_else(|_| TableSchema {
            database: String::new(),
            db_type: String::new(),
            tables: vec![],
        });

        // 构建消息：系统提示 + 澄清上下文
        let messages = vec![
            ChatMsg {
                role: "system".to_string(),
                content: self.build_system_prompt(&schema),
            },
            ChatMsg {
                role: "user".to_string(),
                content: format!(
                    "之前的问题需要澄清。用户对问题 '{}' 的回答是: {}。请根据这个信息生成 SQL 查询。",
                    req.question_id, req.answer
                ),
            },
        ];

        let llm_raw = self.call_llm(messages).await?;
        let parsed = self.parse_llm_response(&llm_raw);

        Ok(NaturalQueryResponse {
            request_id: req.request_id,
            trace_id,
            status: if parsed.sql.is_some() { QueryStatus::Ready } else { QueryStatus::Failed },
            sql: parsed.sql,
            explanation: parsed.explanation,
            confidence: parsed.confidence,
            references: vec![],
            clarification: None,
            lineage_summary: Some(LineageSummary {
                source_tables: parsed.source_tables.unwrap_or_default(),
                key_columns: parsed.key_columns.unwrap_or_default(),
                applied_rules: vec![],
            }),
        })
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

        // 4. 评估风险等级
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
            explain_summary: None,
        })
    }

    // ============== Private Methods ==============

    /// 调用 DeepSeek LLM API（OpenAI 兼容格式）
    async fn call_llm(&self, messages: Vec<ChatMsg>) -> AppResult<String> {
        let url = format!("{}/chat/completions", self.ai_config.llm_base_url);

        info!(
            url = %url,
            model = %self.ai_config.default_model,
            messages_count = messages.len(),
            "调用 DeepSeek LLM"
        );

        let body = ChatCompletionRequest {
            model: self.ai_config.default_model.clone(),
            messages,
            temperature: 0.1,
            max_tokens: self.ai_config.max_tokens,
            response_format: Some(ResponseFormat {
                format_type: "json_object".to_string(),
            }),
        };

        let response = self
            .http_client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.ai_config.llm_api_key),
            )
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, "DeepSeek API 请求失败");
                AppError::ExternalService(format!("DeepSeek API 请求失败: {}", e))
            })?;

        let status = response.status();
        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            error!(status = %status, body = %error_body, "DeepSeek API 返回错误");
            return Err(AppError::ExternalService(format!(
                "DeepSeek API 错误 ({}): {}",
                status, error_body
            )));
        }

        let resp: ChatCompletionResponse = response.json().await.map_err(|e| {
            error!(error = %e, "解析 DeepSeek 响应失败");
            AppError::ExternalService(format!("解析 DeepSeek 响应失败: {}", e))
        })?;

        resp.choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| {
                AppError::ExternalService("DeepSeek 返回了空响应".to_string())
            })
    }

    /// 从 connection-service 获取数据库表结构
    async fn get_schema_info(&self, connection_id: &str) -> AppResult<TableSchema> {
        let url = format!(
            "{}/api/connections/{}/schema",
            self.service_urls.connection_service, connection_id
        );

        info!(url = %url, "获取数据库 Schema");

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| AppError::ExternalService(format!("获取 Schema 失败: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!(status = %status, "获取 Schema 返回非 200: {}", body);
            return Err(AppError::ExternalService(format!(
                "获取 Schema 失败 ({})",
                status
            )));
        }

        let api_resp: ApiResponse<TableSchema> = response.json().await.map_err(|e| {
            AppError::ExternalService(format!("解析 Schema 响应失败: {}", e))
        })?;

        api_resp
            .data
            .ok_or_else(|| AppError::ExternalService("Schema 数据为空".to_string()))
    }

    /// 构建系统提示词
    fn build_system_prompt(&self, schema: &TableSchema) -> String {
        let mut prompt = String::from(
            "你是一个专业的数据库查询助手。你的任务是将用户的自然语言问题转换为准确的 SQL 查询。\n\n\
             ## 规则\n\
             1. 只生成 SELECT 查询（只读操作）\n\
             2. 对于可能返回大量数据的查询，添加合理的 LIMIT 限制\n\
             3. 使用提供的数据库 Schema 中的正确表名和列名\n\
             4. 如果用户的问题不明确，通过 need_clarification 字段请求澄清\n\
             5. 生成的 SQL 应该高效，避免不必要的全表扫描\n\n",
        );

        // 添加数据库信息
        if !schema.db_type.is_empty() {
            prompt.push_str(&format!("## 数据库类型\n{}\n\n", schema.db_type));
        }
        if !schema.database.is_empty() {
            prompt.push_str(&format!("## 数据库名称\n{}\n\n", schema.database));
        }

        // 添加表结构信息
        if !schema.tables.is_empty() {
            prompt.push_str("## 数据库表结构\n");
            for table in &schema.tables {
                prompt.push_str(&format!("\n### 表: {}\n", table.name));
                prompt.push_str("| 列名 | 类型 | 可空 | 键 |\n|------|------|------|-----|\n");
                for col in &table.columns {
                    prompt.push_str(&format!(
                        "| {} | {} | {} | {} |\n",
                        col.name,
                        col.data_type,
                        if col.nullable { "YES" } else { "NO" },
                        col.key.as_deref().unwrap_or("-")
                    ));
                }
            }
            prompt.push('\n');
        } else {
            prompt.push_str("## 注意\n当前没有获取到数据库表结构信息，请根据用户问题尽可能生成通用的 SQL。\n\n");
        }

        // 添加输出格式要求
        prompt.push_str(
            "## 输出格式要求\n\
             请以 JSON 格式回复，包含以下字段：\n\
             ```json\n\
             {\n\
               \"sql\": \"生成的 SQL 查询语句\",\n\
               \"explanation\": \"SQL 的自然语言解释\",\n\
               \"confidence\": 0.0到1.0之间的置信度,\n\
               \"source_tables\": [\"涉及的表名列表\"],\n\
               \"key_columns\": [\"涉及的关键列名\"],\n\
               \"need_clarification\": false,\n\
               \"clarification_question\": null,\n\
               \"clarification_dimension\": null,\n\
               \"clarification_options\": null\n\
             }\n\
             ```\n\n\
             如果需要用户澄清，设置 need_clarification 为 true，并提供 clarification_question。\n",
        );

        prompt
    }

    /// 解析 LLM 返回的 JSON 响应
    fn parse_llm_response(&self, raw: &str) -> LlmSqlResponse {
        // 尝试直接解析 JSON
        if let Ok(parsed) = serde_json::from_str::<LlmSqlResponse>(raw) {
            return parsed;
        }

        // 尝试从 markdown code block 中提取 JSON
        let json_str = if let Some(start) = raw.find("```json") {
            let start = start + 7;
            if let Some(end) = raw[start..].find("```") {
                &raw[start..start + end]
            } else {
                raw
            }
        } else if let Some(start) = raw.find('{') {
            if let Some(end) = raw.rfind('}') {
                &raw[start..=end]
            } else {
                raw
            }
        } else {
            raw
        };

        match serde_json::from_str::<LlmSqlResponse>(json_str.trim()) {
            Ok(parsed) => parsed,
            Err(e) => {
                warn!(error = %e, raw_len = raw.len(), "解析 LLM JSON 响应失败，尝试提取 SQL");
                // 最后兜底：把整个内容当作解释返回
                LlmSqlResponse {
                    explanation: Some(raw.to_string()),
                    ..Default::default()
                }
            }
        }
    }
}
