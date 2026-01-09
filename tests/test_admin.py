from telegram.constants import ChatType

from freedom_trench_bot.bot import is_user_admin


def test_is_user_admin_via_allowlist():
    assert is_user_admin(1, ChatType.GROUP, {1}, None) is True


def test_is_user_admin_private_chat():
    assert is_user_admin(2, ChatType.PRIVATE, set(), {2}) is False


def test_is_user_admin_via_chat_admins():
    assert is_user_admin(3, ChatType.SUPERGROUP, set(), {3}) is True


def test_is_user_admin_missing_admins():
    assert is_user_admin(4, ChatType.GROUP, set(), None) is False
