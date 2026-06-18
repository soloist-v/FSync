use super::*;

fn p(path: &str) -> PathBuf {
    PathBuf::from(path)
}

fn child(parent: &str, path: &str) -> PathBuf {
    PathBuf::from(parent).join(path)
}

fn assert_collapse(input: Vec<FsEvent>, expected: Vec<FsEvent>) {
    assert_eq!(collapse_ops(input), expected);
}

#[test]
fn empty_input_is_noop() {
    assert_collapse(vec![], vec![]);
}

#[test]
fn independent_paths_keep_their_latest_ops() {
    assert_collapse(
        vec![
            FsEvent::Modify(p("A")),
            FsEvent::Create(p("B")),
            FsEvent::Remove(p("C")),
            FsEvent::MkDir(p("D")),
        ],
        vec![
            FsEvent::Remove(p("C")),
            FsEvent::Modify(p("A")),
            FsEvent::Create(p("B")),
            FsEvent::MkDir(p("D")),
        ],
    );
}

#[test]
fn create_modify_collapses_to_create() {
    assert_collapse(
        vec![FsEvent::Create(p("A")), FsEvent::Modify(p("A"))],
        vec![FsEvent::Modify(p("A"))],
    );
}

#[test]
fn modify_create_collapses_to_create() {
    assert_collapse(
        vec![FsEvent::Modify(p("A")), FsEvent::Create(p("A"))],
        vec![FsEvent::Create(p("A"))],
    );
}

#[test]
fn modify_remove_collapses_to_remove() {
    assert_collapse(
        vec![FsEvent::Modify(p("A")), FsEvent::Remove(p("A"))],
        vec![FsEvent::Remove(p("A"))],
    );
}

#[test]
fn create_remove_create_uploads_final_path_once() {
    assert_collapse(
        vec![
            FsEvent::Create(p("A")),
            FsEvent::Remove(p("A")),
            FsEvent::Create(p("A")),
        ],
        vec![FsEvent::Create(p("A"))],
    );
}

#[test]
fn remove_create_remove_deletes_original_once() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::Create(p("A")),
            FsEvent::Remove(p("A")),
        ],
        vec![FsEvent::Remove(p("A"))],
    );
}

#[test]
fn rename_chain_that_returns_to_origin_is_noop() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("A")),
        ],
        vec![],
    );
}

#[test]
fn rename_chain_then_modify_uploads_final_path_and_deletes_origin() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("C")),
            FsEvent::Modify(p("C")),
        ],
        vec![FsEvent::Remove(p("A")), FsEvent::Modify(p("C"))],
    );
}

#[test]
fn modify_then_rename_chain_uploads_final_path_and_deletes_origin() {
    assert_collapse(
        vec![
            FsEvent::Modify(p("A")),
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("C")),
        ],
        vec![FsEvent::Remove(p("A")), FsEvent::Create(p("C"))],
    );
}

#[test]
fn rename_chain_then_remove_deletes_origin() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("C")),
            FsEvent::Remove(p("C")),
        ],
        vec![FsEvent::Remove(p("A"))],
    );
}

#[test]
fn create_rename_modify_uploads_final_path_without_removing_origin() {
    assert_collapse(
        vec![
            FsEvent::Create(p("A")),
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Modify(p("B")),
        ],
        vec![FsEvent::Modify(p("B"))],
    );
}

#[test]
fn remove_then_mkdir_uploads_directory_after_deleting_original_path() {
    assert_collapse(
        vec![FsEvent::Remove(p("A")), FsEvent::MkDir(p("A"))],
        vec![FsEvent::Remove(p("A")), FsEvent::MkDir(p("A"))],
    );
}

#[test]
fn mkdir_then_remove_is_noop() {
    assert_collapse(
        vec![FsEvent::MkDir(p("A")), FsEvent::Remove(p("A"))],
        vec![],
    );
}

#[test]
fn mkdir_then_rename_creates_final_directory_only() {
    assert_collapse(
        vec![FsEvent::MkDir(p("A")), FsEvent::Rename(p("A"), p("B"))],
        vec![FsEvent::MkDir(p("B"))],
    );
}

#[test]
fn mkdir_rename_remove_is_noop() {
    assert_collapse(
        vec![
            FsEvent::MkDir(p("A")),
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Remove(p("B")),
        ],
        vec![],
    );
}

#[test]
fn remove_mkdir_rename_deletes_original_and_creates_final_directory() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("A")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![FsEvent::Remove(p("A")), FsEvent::MkDir(p("B"))],
    );
}

#[test]
fn rename_target_recreated_after_delete_is_independent_create() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Remove(p("B")),
            FsEvent::Create(p("B")),
        ],
        vec![FsEvent::Create(p("B"))],
    );
}

#[test]
fn two_renames_from_different_sources_keep_both_final_paths() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("C"), p("D")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("C"), p("D")),
        ],
    );
}

#[test]
fn recreated_source_after_rename_and_target_remove_deletes_original_and_uploads_source() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Create(p("A")),
            FsEvent::Remove(p("B")),
        ],
        vec![FsEvent::Remove(p("A")), FsEvent::Create(p("A"))],
    );
}

#[test]
fn created_target_overwritten_by_rename_is_not_uploaded() {
    assert_collapse(
        vec![FsEvent::Create(p("B")), FsEvent::Rename(p("A"), p("B"))],
        vec![FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn removed_target_before_rename_is_deleted_before_rename() {
    assert_collapse(
        vec![FsEvent::Remove(p("B")), FsEvent::Rename(p("A"), p("B"))],
        vec![FsEvent::Remove(p("B")), FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn remove_create_target_overwritten_by_rename_deletes_initial_target_only() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("B")),
            FsEvent::Create(p("B")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![FsEvent::Remove(p("B")), FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn create_remove_target_before_rename_does_not_delete_remote_target() {
    assert_collapse(
        vec![
            FsEvent::Create(p("B")),
            FsEvent::Remove(p("B")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn rename_target_chain_overwritten_by_later_rename_deletes_first_origin() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("C"), p("B")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![FsEvent::Remove(p("C")), FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn parent_remove_suppresses_created_child() {
    assert_collapse(
        vec![
            FsEvent::MkDir(p("D")),
            FsEvent::Create(p("D/file.txt")),
            FsEvent::Remove(p("D")),
        ],
        vec![],
    );
}

#[test]
fn parent_remove_suppresses_modified_child_and_removes_parent() {
    assert_collapse(
        vec![FsEvent::Modify(p("D/file.txt")), FsEvent::Remove(p("D"))],
        vec![FsEvent::Remove(p("D"))],
    );
}

#[test]
fn parent_remove_suppresses_child_rename() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("D/a.txt"), p("D/b.txt")),
            FsEvent::Remove(p("D")),
        ],
        vec![FsEvent::Remove(p("D"))],
    );
}

#[test]
fn three_step_rename_cycle_is_noop() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("C")),
            FsEvent::Rename(p("C"), p("A")),
        ],
        vec![],
    );
}

#[test]
fn reused_source_after_rename_can_cancel_without_extra_create() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Create(p("A")),
            FsEvent::Modify(p("A")),
            FsEvent::Remove(p("A")),
        ],
        vec![FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn remove_mkdir_remove_create_uploads_file_after_deleting_original() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("A")),
            FsEvent::Remove(p("A")),
            FsEvent::Create(p("A")),
        ],
        vec![FsEvent::Remove(p("A")), FsEvent::Create(p("A"))],
    );
}

#[test]
fn modified_target_overwritten_by_rename_discards_target_modify() {
    assert_collapse(
        vec![FsEvent::Modify(p("B")), FsEvent::Rename(p("A"), p("B"))],
        vec![FsEvent::Remove(p("B")), FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn mkdir_target_overwritten_by_rename_is_not_uploaded() {
    assert_collapse(
        vec![FsEvent::MkDir(p("B")), FsEvent::Rename(p("A"), p("B"))],
        vec![FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn remove_mkdir_target_overwritten_by_rename_deletes_initial_target_only() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("B")),
            FsEvent::MkDir(p("B")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![FsEvent::Remove(p("B")), FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn parent_rename_rewrites_modified_child_to_new_parent() {
    assert_collapse(
        vec![
            FsEvent::Modify(p("D/a.txt")),
            FsEvent::Rename(p("D"), p("E")),
        ],
        vec![
            FsEvent::Rename(p("D"), p("E")),
            FsEvent::Modify(child("E", "a.txt")),
        ],
    );
}

#[test]
fn parent_rename_rewrites_created_child_to_new_parent() {
    assert_collapse(
        vec![
            FsEvent::Create(p("D/new.txt")),
            FsEvent::Rename(p("D"), p("E")),
        ],
        vec![
            FsEvent::Rename(p("D"), p("E")),
            FsEvent::Create(child("E", "new.txt")),
        ],
    );
}

#[test]
fn parent_rename_rewrites_removed_child_to_new_parent() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("D/a.txt")),
            FsEvent::Rename(p("D"), p("E")),
        ],
        vec![
            FsEvent::Rename(p("D"), p("E")),
            FsEvent::Remove(child("E", "a.txt")),
        ],
    );
}

#[test]
fn child_moved_out_before_parent_remove_survives_parent_remove() {
    assert_collapse(
        vec![FsEvent::Rename(p("D/sub"), p("X")), FsEvent::Remove(p("D"))],
        vec![FsEvent::Remove(p("D")), FsEvent::Rename(p("D/sub"), p("X"))],
    );
}

#[test]
fn file_name_swap_uses_observed_temp_path_sequence() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("tmp")),
            FsEvent::Rename(p("B"), p("A")),
            FsEvent::Rename(p("tmp"), p("B")),
        ],
        vec![
            FsEvent::Rename(p("B"), p("A")),
            FsEvent::Rename(p("A"), p("B")),
        ],
    );
}

#[test]
fn nested_parent_rename_rewrites_deep_child_modify() {
    assert_collapse(
        vec![
            FsEvent::Modify(p("A/B/c.txt")),
            FsEvent::Rename(p("A"), p("X")),
            FsEvent::Rename(p("X/B"), p("Y")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("X")),
            FsEvent::Rename(child("X", "B"), p("Y")),
            FsEvent::Modify(child("Y", "c.txt")),
        ],
    );
}

#[test]
fn nested_parent_remove_suppresses_rewritten_child_create() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Create(p("A/new.txt")),
            FsEvent::Remove(p("B")),
        ],
        vec![FsEvent::Remove(p("A"))],
    );
}

#[test]
fn child_created_after_parent_rename_uses_new_parent() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Create(p("B/new.txt")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Create(child("B", "new.txt")),
        ],
    );
}

#[test]
fn child_modified_after_parent_rename_uses_new_parent() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Modify(p("B/existing.txt")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Modify(child("B", "existing.txt")),
        ],
    );
}

#[test]
fn moved_out_child_modified_after_parent_remove_uploads_moved_child() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A/sub"), p("X")),
            FsEvent::Remove(p("A")),
            FsEvent::Modify(p("X/file.txt")),
        ],
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::Rename(p("A/sub"), p("X")),
            FsEvent::Modify(child("X", "file.txt")),
        ],
    );
}

#[test]
fn removed_parent_then_moved_in_child_uploads_child_after_delete() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::Rename(p("B/file.txt"), p("A/file.txt")),
        ],
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::Rename(p("B/file.txt"), child("A", "file.txt")),
        ],
    );
}

#[test]
fn rename_parent_over_existing_dir_removes_target_then_renames_parent() {
    assert_collapse(
        vec![FsEvent::MkDir(p("B")), FsEvent::Rename(p("A"), p("B"))],
        vec![FsEvent::Rename(p("A"), p("B"))],
    );
}

#[test]
fn remove_mkdir_parent_then_child_create_preserves_parent_delete() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("A")),
            FsEvent::Create(p("A/file.txt")),
        ],
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("A")),
            FsEvent::Create(child("A", "file.txt")),
        ],
    );
}

#[test]
fn mkdir_parent_then_child_create_then_parent_rename_uploads_tree_at_final_parent() {
    assert_collapse(
        vec![
            FsEvent::MkDir(p("A")),
            FsEvent::Create(p("A/file.txt")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![FsEvent::MkDir(p("B"))],
    );
}

#[test]
fn parent_rename_cycle_with_child_modify_rewrites_to_original_parent() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Modify(p("B/file.txt")),
            FsEvent::Rename(p("B"), p("A")),
        ],
        vec![FsEvent::Modify(child("A", "file.txt"))],
    );
}

#[test]
fn overwritten_renamed_target_then_recreate_target_keeps_final_create_only() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("C"), p("B")),
            FsEvent::Remove(p("B")),
            FsEvent::Create(p("B")),
        ],
        vec![FsEvent::Remove(p("A")), FsEvent::Create(p("B"))],
    );
}

#[test]
fn dirty_source_renamed_over_removed_target_removes_both_and_uploads_final() {
    assert_collapse(
        vec![
            FsEvent::Modify(p("A")),
            FsEvent::Remove(p("B")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![
            FsEvent::Remove(p("B")),
            FsEvent::Remove(p("A")),
            FsEvent::Create(p("B")),
        ],
    );
}

#[test]
fn sibling_with_common_text_prefix_is_not_treated_as_child() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("app")),
            FsEvent::Modify(p("app-cache/file.txt")),
        ],
        vec![
            FsEvent::Remove(p("app")),
            FsEvent::Modify(child("app-cache", "file.txt")),
        ],
    );
}

#[test]
fn sibling_directory_rename_does_not_rewrite_common_prefix_path() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("app"), p("src")),
            FsEvent::Modify(p("app-cache/file.txt")),
        ],
        vec![
            FsEvent::Rename(p("app"), p("src")),
            FsEvent::Modify(child("app-cache", "file.txt")),
        ],
    );
}

#[test]
fn child_remove_after_two_parent_renames_uses_final_parent_path() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("C")),
            FsEvent::Remove(p("C/file.txt")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("C")),
            FsEvent::Remove(child("C", "file.txt")),
        ],
    );
}

#[test]
fn stale_child_create_under_original_parent_is_rewritten_through_parent_chain() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("C")),
            FsEvent::Create(p("A/new.txt")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("C")),
            FsEvent::Create(child("C", "new.txt")),
        ],
    );
}

#[test]
fn stale_child_modify_under_original_parent_is_rewritten_through_parent_chain() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Rename(p("B"), p("C")),
            FsEvent::Modify(p("A/existing.txt")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("C")),
            FsEvent::Modify(child("C", "existing.txt")),
        ],
    );
}

#[test]
fn removed_parent_recreated_as_directory_then_renamed_preserves_delete_and_final_tree_upload() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("A")),
            FsEvent::Create(p("A/file.txt")),
            FsEvent::Rename(p("A"), p("B")),
        ],
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("B")),
            FsEvent::Create(child("B", "file.txt")),
        ],
    );
}

#[test]
fn dirty_child_inside_parent_rename_then_parent_renamed_again_does_not_delete_original_child() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Modify(p("B/dir/file.txt")),
            FsEvent::Rename(p("B"), p("C")),
        ],
        vec![
            FsEvent::Rename(p("A"), p("C")),
            FsEvent::Modify(p("C/dir/file.txt")),
        ],
    );
}

#[test]
fn moved_out_child_then_old_parent_recreated_keeps_child_move_and_new_parent_upload() {
    assert_collapse(
        vec![
            FsEvent::Rename(p("A/sub"), p("X")),
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("A")),
            FsEvent::Create(p("A/new.txt")),
        ],
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::Rename(p("A/sub"), p("X")),
            FsEvent::MkDir(p("A")),
            FsEvent::Create(child("A", "new.txt")),
        ],
    );
}

#[test]
fn overwrite_dirty_renamed_entity_with_local_file_removes_original_and_uploads_replacement() {
    assert_collapse(
        vec![
            FsEvent::Modify(p("A")),
            FsEvent::Rename(p("A"), p("B")),
            FsEvent::Create(p("B")),
        ],
        vec![FsEvent::Remove(p("A")), FsEvent::Create(p("B"))],
    );
}

#[test]
fn remove_file_then_mkdir_then_child_create_then_remove_parent_deletes_original_once() {
    assert_collapse(
        vec![
            FsEvent::Remove(p("A")),
            FsEvent::MkDir(p("A")),
            FsEvent::Create(p("A/file.txt")),
            FsEvent::Remove(p("A")),
        ],
        vec![FsEvent::Remove(p("A"))],
    );
}
