// SPDX-License-Identifier: MIT

pragma solidity 0.8.16;


contract TicTacToeGame {

    address public _playerOne = msg.sender;
    address public _playerTwo = address(0);
    address private _lastPlayed = address(0);
    address public _winner = address(0);
    bool public _isGameOver = false;
    uint8 private _turnsTaken = 0;

    //GameBoard is a 1D array having the location indexes as 
    /*  
        0   1   2
        3   4   5
        6   7   8
    */
    address[9] private _gameBoard;
    
    //Function will start the game by taking the address of the second player. 
    //The address of the first player will be the same one which will initiate the game.
    function startGame(address _player2) external {
        /*Add you code here*/
        // check if player one and player 2 are not same.
        require(_playerOne != _player2,"Player1 and Player2 cannot be same.");
        _playerTwo = _player2;
        
    }
    

    //Function for placing the move of the current player on the game board
    function placeMove(uint8 _location) external {

        //This will check if the game is over or is still active by checking the isGameOver flag and the winner address
        /*Add you code here*/ 
        require(_isGameOver == false || _winner == address(0), "Game is over and is already won.");
        
        //This will check if the game is draw or is still active by checking the isGameOver flag
        /*Add you code here*/
        require(_isGameOver == false,"Game is not active as its a Draw.");
            
        //This will check if the transaction is made from some other system(having different address other then that of players) apart from both the players
        /*Add you code here*/
        require(msg.sender == _playerOne || msg.sender == _playerTwo,"Only players registered for game can play it.");
                
        //While placing the move, we will check if the move at the specified location is already taken of not
        /*Add you code here*/
        require(_gameBoard[_location] == address(0),"Make your moves only on empty locations.");
                    
        //This condition is to check if the last played player is again the turn or is the turn given to the next player inline
        /*Add you code here*/
        require(_lastPlayed != msg.sender,"One player can make only 1 move at a time. And move can be made only if another player have made their move.");
                        
        //Saving the player's address on the required location.
        /*Add you code here*/
        _gameBoard[_location] = msg.sender;                               

        //Saving the current player's address in the last played variable so as to keep the track of the latest plater which exercised the move
        /*Add you code here*/
        _lastPlayed = msg.sender;

        //Tracking the number of turns
        /*Add you code here*/
        _turnsTaken += 1;



        //Checking if the game lead to the winner after the current move and accordingly updating the winner and isGameOver flag
        /*Add you code here*/
        if (isWinner(msg.sender) == true)
        {
            _isGameOver = true;
            _winner = msg.sender;
        }


        //For checking if the game is draw
        if (_turnsTaken == 9 && _isGameOver == false) 
            {
                _isGameOver = true;
            }
    }
    
    //Function for checking if we have a winner of the game
    function isWinner(address player) private view returns(bool) {
        //various winning filters in terms of rows, columns and diagonals
        uint8[3][8] memory winningfilters = [
            [0,1,2],[3,4,5],[6,7,8],  // winning row filters
            [0,3,6],[1,4,7],[2,5,8],  // winning column filters
            [0,4,8],[6,4,2]           // winning diagonal filters
        ];
        
        /*Add you code here*/
        bool winning = false;
        uint8 w=0;
        while (w<8)
        {
            if (_gameBoard[winningfilters[w][0]] == player && _gameBoard[winningfilters[w][1]] == player && _gameBoard[winningfilters[w][2]] == player)
                {
                    winning = true;                    
                }
            w = w + 1;
        }
        return winning;
    }
    
    //Function which returns the game board view 
    function getBoard() external view returns(string memory) {
        string memory gameBoardView;
        for(uint8 i = 0; i<_gameBoard.length; i++){
            gameBoardView = string(abi.encodePacked(bytes(gameBoardView), bytes(getLocationShape(_gameBoard[i])), " "));
            if((i+1)%3 == 0 && i != 0 && i != 8){
                gameBoardView = string(abi.encodePacked(bytes(gameBoardView), " | "));
            }
        }
        return gameBoardView;
    }

    //Function for checking the game board location is having the player one's address or player two's address
    function getLocationShape(address addressStored) private view returns(string memory){
        if(addressStored == address(0)){
            return "-";
        } else if(addressStored == _playerOne){
            return "X";
        } else{
            return "O";
        }
    }
}