%% The MIT License

%% Copyright (c) 2010-2013 alisdair sullivan <alisdairsullivan@yahoo.ca>

%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:

%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.

%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.


-module(jsx_stream).

-export([stream/1, stream/2]).
-export([init/1, handle_event/2]).

-ifdef(TEST).
-export([handle_event/3, end_stream/1]).
-endif.


-spec stream(Handler::module()) -> fun((binary()) -> any()).
-spec stream(Handler::module(), Config::list()) -> fun((binary()) -> any()).

stream(Handler) -> stream(Handler, []).
stream(Handler, Config) ->
    fun(Binary) when is_binary(Binary) ->
        try (jsx:decoder(
            ?MODULE,
            {Handler, Config},
            [stream] ++ jsx_config:extract_config(Config)
        ))(Binary)
        catch throw:{halt, State} -> State
        end
    end.


-type state() :: {Handler::module(), Path::list(), State::any()}.
-spec init({Handler::module(), Config::list()}) -> state().

init({Handler, Config}) -> {Handler, [], Handler:init(Config)};
% this is for the test suite, do not remove
init(_) -> [].


-spec handle_event(Event::any(), State::state()) -> state().

handle_event(end_json, {Handler, _, State}) ->
    Handler:end_stream(State);

handle_event({key, Key}, {Handler, Path, State}) ->
    {Handler, Path ++ [Key], State};

handle_event(end_object, {Handler, Path, State}) ->
    {Handler, update_path(Path), State};

handle_event(end_array, {Handler, Path, State}) ->
    {Handler, update_path(drop(Path)), State};

handle_event(start_array, {Handler, Path, State}) ->
    {Handler, Path ++ [0], State};

handle_event({_, Event}, {Handler, Path, State}) ->
    NewPath = update_path(Path),
    case Handler:handle_event(Path, Event, State) of
        {ok, NewState} -> {Handler, NewPath, NewState};
        {halt, NewState} -> throw({halt, Handler:end_stream(NewState)})
    end;

handle_event({incomplete, F}, _) ->
    fun(Binary) when is_binary(Binary) ->
        try F(Binary)
        catch throw:{halt, State} -> State
        end
    end;

handle_event(_, State) -> State.


update_path([]) -> [];
update_path(Path) ->
    case lists:last(Path) of
        X when is_integer(X) -> drop(Path) ++ [X+1];
        _ -> drop(Path)
    end.


drop(Path) -> droplast(Path).


% from stdlib, not available prior to 17.1?
droplast([_T]) -> [];
droplast([H|T]) -> [H|droplast(T)].


%% eunit tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

handle_event(Path, Event, State) -> {ok, State ++ [{Path, Event}]}.

end_stream(State) -> State.

-define(STREAM(JSON), begin {incomplete, F} = (jsx:stream(?MODULE))(JSON), F(end_stream) end).

stream_test_() ->
    [
        {"empty object", ?_assertEqual([], ?STREAM(<<"{}">>))},
        {"empty list", ?_assertEqual([], ?STREAM(<<"[]">>))},
        {"simple object", ?_assertEqual([{[<<"key">>], true}], ?STREAM(<<"{\"key\": true}">>))},
        {"simple list", ?_assertEqual([
                {[0], true},
                {[1], false},
                {[2], null}
            ], ?STREAM(<<"[true, false, null]">>)
        )},
        {"complex json", ?_assertEqual(
            [
                {[<<"library">>], <<"erlang programming books">>},
                {[<<"books">>, 0, <<"title">>], <<"learn you some erlang">>},
                {[<<"books">>, 0, <<"authors">>, 0], <<"fred hebert">>},
                {[<<"books">>, 0, <<"publication">>], <<"2013">>},
                {[<<"books">>, 1, <<"title">>], <<"programming erlang">>},
                {[<<"books">>, 1, <<"authors">>, 0], <<"joe armstrong">>},
                {[<<"books">>, 1, <<"publication">>], <<"2013">>},
                {[<<"books">>, 2, <<"title">>], <<"erlang programming">>},
                {[<<"books">>, 2, <<"authors">>, 0], <<"francesco cesarini">>},
                {[<<"books">>, 2, <<"authors">>, 1], <<"simon thompson">>},
                {[<<"books">>, 2, <<"publication">>], <<"2009">>},
                {[<<"location">>], <<"amazon">>}
            ],
            ?STREAM(<<"{
                \"library\": \"erlang programming books\",
                \"books\": [
                    {
                        \"title\": \"learn you some erlang\",
                        \"authors\": [\"fred hebert\"],
                        \"publication\": \"2013\"
                    },
                    {
                        \"title\": \"programming erlang\",
                        \"authors\": [\"joe armstrong\"],
                        \"publication\": \"2013\"
                    },
                    {
                        \"title\": \"erlang programming\",
                        \"authors\": [\"francesco cesarini\", \"simon thompson\"],
                        \"publication\": \"2009\"
                    }
                ],
                \"location\": \"amazon\"
            }">>)
        )}
    ].

-endif.
