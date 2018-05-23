from platform_api.orchestrator.job_request import Container


class TestContainer:
    def test_command_list_empty(self):
        container = Container(image='testimage')
        assert container.command_list == []

    def test_command_list(self):
        container = Container(image='testimage', command='bash -c date')
        assert container.command_list == ['bash', '-c', 'date']
