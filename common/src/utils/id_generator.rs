//! Unique ID generator.
//!
//! Provides utilities for generating unique identifiers.

use uuid::Uuid;

/// Generates unique identifiers for various entities.
pub struct IdGenerator;

impl IdGenerator {
    /// Generates a unique connection ID.
    ///
    /// # Returns
    /// A unique UUID string.
    pub fn connection_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// Generates a unique request ID.
    ///
    /// # Returns
    /// A unique UUID string.
    pub fn request_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// Generates a short unique ID (first 8 characters of UUID).
    ///
    /// # Returns
    /// An 8-character unique string.
    pub fn short_id() -> String {
        Uuid::new_v4().to_string()[..8].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_id_is_unique() {
        let id1 = IdGenerator::connection_id();
        let id2 = IdGenerator::connection_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_short_id_length() {
        let id = IdGenerator::short_id();
        assert_eq!(id.len(), 8);
    }
}
