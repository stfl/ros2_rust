use crate::{
    node::NodeHandle, rcl_bindings::*, MessageInfo, RclrsError, ToResult, ENTITY_LIFECYCLE_MUTEX,
};
use std::{ptr, sync::Arc};

use crate::serialized_message::SerializedMessage;

/// A subscription which receives serialized ROS messages.
pub struct SerializedSubscription {
    pub(crate) handle: Arc<NodeHandle>,
    pub(crate) sub: rcl_subscription_t,
    // Keeps the `rosidl_typesupport_c` library loaded for as long as the
    // subscription lives: `sub` was initialized with a type support pointer that
    // points into this library, so unloading it earlier would dangle.
    #[allow(dead_code)]
    pub(crate) type_support_library: Arc<libloading::Library>,
}

// SAFETY: `rcl_subscription_t` holds raw pointers, so it is neither `Send` nor
// `Sync` automatically. It is safe to move to another thread (the subscription
// has a single owner). It is also safe to share a `&SerializedSubscription`
// across threads: every operation that touches `sub` — `take` and `drop` — takes
// `&mut self`, so a shared reference can never reach `rcl_take_serialized_message`
// or `rcl_subscription_fini` (neither of which is safe to call concurrently on
// one subscription). Requiring `&mut self` enforces that exclusivity at compile
// time, which is what makes `Sync` sound here *without* an internal lock — the
// alternative (a `Mutex<rcl_subscription_t>` with a `&self` take) would pay a
// lock on every poll.
unsafe impl Send for SerializedSubscription {}
unsafe impl Sync for SerializedSubscription {}

impl Drop for SerializedSubscription {
    fn drop(&mut self) {
        let _context_lock = self.handle.context_handle.rcl_context.lock().unwrap();
        let mut node = self.handle.rcl_node.lock().unwrap();
        let _lifecycle_lock = ENTITY_LIFECYCLE_MUTEX.lock().unwrap();
        unsafe {
            let _ = rcl_subscription_fini(&mut self.sub, &mut *node);
        }
    }
}

impl SerializedSubscription {
    /// Take the next serialized (CDR) message into `buf`, if one is queued.
    ///
    /// Returns `Ok(Some(info))` when a message was taken, and `Ok(None)` when the
    /// middleware simply has nothing queued right now
    /// (`RCL_RET_SUBSCRIPTION_TAKE_FAILED`, which is the normal "empty" signal, not
    /// an error). Any *other* rcl failure is returned as `Err` rather than being
    /// silently swallowed as "no message".
    ///
    /// Takes `&mut self`: a serialized subscription has a single consumer and
    /// `rcl_take_serialized_message` is not safe to call concurrently on the same
    /// subscription (see the `Send`/`Sync` note above).
    pub fn take(&mut self, buf: &mut SerializedMessage) -> Result<Option<MessageInfo>, RclrsError> {
        let mut info: rmw_message_info_t = unsafe { std::mem::zeroed() };
        // SAFETY: `sub` and `buf.msg` are initialized; the message-info pointer is
        // valid for the call; the allocation pointer is allowed to be null.
        let result = unsafe {
            rcl_take_serialized_message(&self.sub, &mut buf.msg, &mut info, ptr::null_mut()).ok()
        };
        match result {
            Ok(()) => Ok(Some(MessageInfo::from_rmw_message_info(&info))),
            Err(err) if err.is_take_failed() => Ok(None),
            Err(err) => Err(err),
        }
    }
}
