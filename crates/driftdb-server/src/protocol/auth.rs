//! PostgreSQL Authentication

use md5;
use hex;

/// Authentication methods supported by DriftDB
#[derive(Debug, Clone)]
pub enum AuthMethod {
    Trust,      // No authentication
    Password,   // Clear text password
    MD5,        // MD5 hashed password
}

/// Perform MD5 authentication as per PostgreSQL protocol
pub fn md5_auth(password: &str, username: &str, salt: &[u8; 4]) -> String {
    // PostgreSQL MD5 auth:
    // 1. MD5(password + username)
    // 2. MD5(result + salt)
    // 3. Prepend "md5"

    let pass_user = format!("{}{}", password, username);
    let pass_user_hash = md5::compute(pass_user.as_bytes());

    let salt_input = format!("{}{}", hex::encode(pass_user_hash.as_ref()),
                            std::str::from_utf8(salt).unwrap_or(""));
    let final_hash = md5::compute(salt_input.as_bytes());

    format!("md5{}", hex::encode(final_hash.as_ref()))
}

/// Verify MD5 authentication
pub fn verify_md5(
    received: &str,
    expected_password: &str,
    username: &str,
    salt: &[u8; 4],
) -> bool {
    let expected = md5_auth(expected_password, username, salt);
    received == expected
}

/// Simple user database (in production, use proper storage)
pub struct UserDb {
    users: std::collections::HashMap<String, User>,
}

#[derive(Clone)]
struct User {
    password: String,
    is_superuser: bool,
}

impl UserDb {
    pub fn new() -> Self {
        let mut users = std::collections::HashMap::new();

        // Default users
        users.insert("driftdb".to_string(), User {
            password: "driftdb".to_string(),
            is_superuser: true,
        });

        users.insert("readonly".to_string(), User {
            password: "readonly".to_string(),
            is_superuser: false,
        });

        Self { users }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        self.users.get(username)
            .map(|user| user.password == password)
            .unwrap_or(false)
    }

    pub fn authenticate_md5(&self, username: &str, received: &str, salt: &[u8; 4]) -> bool {
        self.users.get(username)
            .map(|user| verify_md5(received, &user.password, username, salt))
            .unwrap_or(false)
    }

    pub fn is_superuser(&self, username: &str) -> bool {
        self.users.get(username)
            .map(|user| user.is_superuser)
            .unwrap_or(false)
    }
}