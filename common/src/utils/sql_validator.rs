//! SQL statement validator.
//!
//! Provides security validation for SQL statements.

use crate::errors::AppError;

/// Validates SQL statements for security.
pub struct SqlValidator;

/// List of forbidden SQL keywords for security.
const FORBIDDEN_KEYWORDS: [&str; 4] = ["DROP ", "TRUNCATE ", "DELETE FROM", "ALTER "];

impl SqlValidator {
    /// Validates a SQL statement for forbidden operations.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to validate
    ///
    /// # Returns
    /// `Ok(())` if the statement is safe, or an error if forbidden keywords are found.
    ///
    /// # Errors
    /// Returns `AppError::UnsafeSql` if the SQL contains forbidden keywords.
    pub fn validate(sql: &str) -> Result<(), AppError> {
        let sql_upper = sql.to_uppercase();
        for keyword in FORBIDDEN_KEYWORDS {
            if sql_upper.contains(keyword) {
                return Err(AppError::UnsafeSql(format!(
                    "forbidden operation: {}",
                    keyword.trim()
                )));
            }
        }
        Ok(())
    }

    /// Checks if the SQL is a SELECT query.
    pub fn is_select(sql: &str) -> bool {
        sql.trim().to_uppercase().starts_with("SELECT")
    }

    /// Checks if the SQL is a modification query (INSERT/UPDATE/DELETE).
    pub fn is_modification(sql: &str) -> bool {
        let sql_upper = sql.trim().to_uppercase();
        sql_upper.starts_with("INSERT")
            || sql_upper.starts_with("UPDATE")
            || sql_upper.starts_with("DELETE")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_is_allowed() {
        assert!(SqlValidator::validate("SELECT * FROM users").is_ok());
    }

    #[test]
    fn test_drop_is_forbidden() {
        assert!(SqlValidator::validate("DROP TABLE users").is_err());
    }

    #[test]
    fn test_is_select() {
        assert!(SqlValidator::is_select("SELECT * FROM users"));
        assert!(!SqlValidator::is_select("INSERT INTO users"));
    }
}
