use crate::{node::NodeHandle, rcl_bindings::*, RclrsError, ToResult, ENTITY_LIFECYCLE_MUTEX};
use std::{ptr, sync::Arc};

use crate::serialized_message::SerializedMessage;

/// A publisher which publishes serialized ROS messages.
pub struct SerializedPublisher {
    pub(crate) handle: Arc<NodeHandle>,
    pub(crate) pub_: rcl_publisher_t,
    // Keeps the `rosidl_typesupport_c` library loaded for as long as the publisher
    // lives: `pub_` was initialized with a type support pointer that points into
    // this library, so unloading it earlier would dangle.
    #[allow(dead_code)]
    pub(crate) type_support_library: Arc<libloading::Library>,
}

unsafe impl Send for SerializedPublisher {}
unsafe impl Sync for SerializedPublisher {}

impl Drop for SerializedPublisher {
    fn drop(&mut self) {
        let _context_lock = self.handle.context_handle.rcl_context.lock().unwrap();
        let mut node = self.handle.rcl_node.lock().unwrap();
        let _lifecycle_lock = ENTITY_LIFECYCLE_MUTEX.lock().unwrap();
        unsafe {
            let _ = rcl_publisher_fini(&mut self.pub_, &mut *node);
        }
    }
}

impl SerializedPublisher {
    /// Publish a serialized (CDR) message.
    pub fn publish(&self, msg: &SerializedMessage) -> Result<(), RclrsError> {
        unsafe {
            rcl_publish_serialized_message(&self.pub_, &msg.msg, ptr::null_mut()).ok()?;
        }
        Ok(())
    }
}
