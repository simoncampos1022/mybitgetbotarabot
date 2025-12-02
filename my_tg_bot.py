import requests
from datetime import datetime, timezone, timedelta
import csv
import os
import time
import numpy as np
import threading
from queue import Queue
import pandas as pd

import bitget.v2.mix.account_api as maxAccountApi
import bitget.v2.mix.market_api as maxMarketApi
import bitget.v2.mix.order_api as maxOrderApi

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import asyncio

PRODUCT_TYPE = "USDT-FUTURES"
MARGIN_COIN = "USDT"
MARGIN_MODE = "isolated"
CSV_FILE = "my_bitget_history.csv"

TELEGRAM_BOT_TOKEN = "8321838467:AAG4s4t8C0Wux71TIq-Dx86NsC0wk3uE6E8"
AUTHORIZED_CHAT_ID = "7839829083"  # Only this chat ID can use the bot

LEVERAGE = 5
INTERVAL = "1H"
FEE_PERCENT = 0.0006
ATR_LENGTH = 14
FS_LENGTH = 10
RSI_LENGTH = 14
MAX_OPEN_POSITIONS = 4  # Maximum number of open positions allowed

# Symbol Configuration
# Each symbol has: parameters (10 levels) and operators (6 operators: 3 for long, 3 for short)
# Operators: '>', '<', '>=', '<=', '==', '!='
# Format: (rsi_operator, fs_operator, vol_operator)
SYMBOL_CONFIG = {
    "ETHUSDT": {
        "long_fs_entry_level": 1.9,
        "long_rsi_entry_level": 29.0,
        "long_stop_loss_level": 3.0,
        "long_take_profit_level": 2.9,
        "long_second_sl_level": 0.7,
        "long_operators": ("<", ">", ">"),  # (rsi_op, fs_op, vol_op)
        "short_fs_entry_level": 4.2,
        "short_rsi_entry_level": 26.0,
        "short_stop_loss_level": 1.7,
        "short_take_profit_level": 0.2,
        "short_second_sl_level": 0.1,
        "short_operators": ("<", "<", ">"),  # (rsi_op, fs_op, vol_op)
    },
    "SOLUSDT": {
        "long_fs_entry_level": 3.2,
        "long_rsi_entry_level": 9.0,
        "long_stop_loss_level": 2.4,
        "long_take_profit_level": 2.3,
        "long_second_sl_level": 0.2,
        "long_operators": ("<", "<", ">"),  # (rsi_op, fs_op, vol_op)
        "short_fs_entry_level": 1.6,
        "short_rsi_entry_level": 1.0,
        "short_stop_loss_level": 1.5,
        "short_take_profit_level": 1.8,
        "short_second_sl_level": 2.9,
        "short_operators": (">", "<", ">"),  # (rsi_op, fs_op, vol_op)
    },
    "TAOUSDT": {
        "long_fs_entry_level": 4.8,
        "long_rsi_entry_level": 13.0,
        "long_stop_loss_level": 2.6,
        "long_take_profit_level": 1.8,
        "long_second_sl_level": 0.1,
        "long_operators": (">", "<", ">"),  # (rsi_op, fs_op, vol_op)
        "short_fs_entry_level": 2.5,
        "short_rsi_entry_level": 2.0,
        "short_stop_loss_level": 3.0,
        "short_take_profit_level": 1.7,
        "short_second_sl_level": 0.1,
        "short_operators": (">", "<", ">"),  # (rsi_op, fs_op, vol_op)
    },
    "LINKUSDT": {
        "long_fs_entry_level": 1.5,
        "long_rsi_entry_level": 9.0,
        "long_stop_loss_level": 3.0,
        "long_take_profit_level": 3.0,
        "long_second_sl_level": 1.3,
        "long_operators": ("<", ">", ">"),  # (rsi_op, fs_op, vol_op)
        "short_fs_entry_level": 2.0,
        "short_rsi_entry_level": 10.0,
        "short_stop_loss_level": 2.0,
        "short_take_profit_level": 1.9,
        "short_second_sl_level": 0.1,
        "short_operators": ("<", "<", "<"),  # (rsi_op, fs_op, vol_op)
    },
    "SUSHIUSDT": {
        "long_fs_entry_level": 1.9,
        "long_rsi_entry_level": 11.0,
        "long_stop_loss_level": 2.6,
        "long_take_profit_level": 3.0,
        "long_second_sl_level": 3.0,
        "long_operators": ("<", ">", ">"),  # (rsi_op, fs_op, vol_op)
        "short_fs_entry_level": 1.6,
        "short_rsi_entry_level": 27.0,
        "short_stop_loss_level": 1.8,
        "short_take_profit_level": 3.0,
        "short_second_sl_level": 0.1,
        "short_operators": ("<", "<", "<"),  # (rsi_op, fs_op, vol_op)
    },
    "UNIUSDT": {
        "long_fs_entry_level": 1.1,
        "long_rsi_entry_level": 28.0,
        "long_stop_loss_level": 2.9,
        "long_take_profit_level": 1.1,
        "long_second_sl_level": 0.1,
        "long_operators": ("<", "<", ">"),  # (rsi_op, fs_op, vol_op)
        "short_fs_entry_level": 4.7,
        "short_rsi_entry_level": 1.0,
        "short_stop_loss_level": 2.9,
        "short_take_profit_level": 2.0,
        "short_second_sl_level": 0.1,
        "short_operators": (">", "<", ">"),  # (rsi_op, fs_op, vol_op)
    },
}

# List of symbols to trade
TRADING_SYMBOLS = list(SYMBOL_CONFIG.keys())

class AutoTradeBot:
    def __init__(self):
        self.flag_api_sent = True
        self.running = True
        self.trade_lock = threading.Lock()
        self.price_lock = threading.Lock()
        
        # Per-symbol data storage
        self.symbol_data = {}
        for symbol in TRADING_SYMBOLS:
            self.symbol_data[symbol] = {
                'candles': [],
                'fs': [],
                'tr': [],
                'current_price': None,
                'current_atr_value': 0.0,
                'current_vol_os': 0.0,
                'current_rsi_value': 0.0,
                'long_position': None,
                'short_position': None,
            }
        
        self.total_trades = 1
        self.trades = []

        self.api_key = "bg_439b475bb0ff165939418f3c5d546e52"
        self.secret_key = "212a66d931d0a3218183e8b82f25d89f4883c41135202f51d230e49d44dffc53"
        self.passphrase = "Simon2004"

        self.maxAccountApi = maxAccountApi.AccountApi(self.api_key, self.secret_key, self.passphrase)
        self.maxMarketApi = maxMarketApi.MarketApi(self.api_key, self.secret_key, self.passphrase)
        self.maxOrderApi = maxOrderApi.OrderApi(self.api_key, self.secret_key, self.passphrase)

        self.price_queue = Queue()
        self.price_update_event = threading.Event()
        self.orderType = "market"
        self.telegram_app = None
        self.message_queue = Queue()
        self.balance = self.fetch_real_balance()
        self.initial_balance = self.balance if self.balance is not None else 100.0
        
        # Set leverage for all symbols
        for symbol in TRADING_SYMBOLS:
            self.set_leverage(LEVERAGE, symbol)

        # Initialize prices for all symbols
        for symbol in TRADING_SYMBOLS:
            price = self.get_current_price(symbol)
            if price is None:
                print(f"[INIT] Initial price from REST for {symbol}: None")
            else:
                print(f"[INIT] Initial price from REST for {symbol}: ${price:.4f}")
                self.symbol_data[symbol]['current_price'] = price
        
        # Load existing trades from CSV
        if os.path.exists(CSV_FILE):
            try:
                with open(CSV_FILE, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        symbol = row.get('symbol')  # Default to first symbol for old data
                        trade_data = {
                            'symbol': symbol,
                            'entry_time': row['entry_time'],
                            'exit_time': row['exit_time'],
                            'action': row['action'],
                            'entry_price': float(row['entry_price']),
                            'exit_price': float(row['exit_price']) if row['exit_price'].strip() else None,
                            'size': float(row['size']),
                            'status': row['status'],
                            'pnl': float(row['pnl']) if row['pnl'] else 0.0,
                            'fee': float(row.get('fee', 0.0)),
                            'ideal_pnl': float(row.get('ideal_pnl', 0.0)),
                            'reason': row.get('reason', ''),
                            'stop_loss': float(row.get('stop_loss', 0.0)),
                            'take_profit': float(row.get('take_profit', 0.0)),
                            'max_profit_price': float(row.get('max_profit_price', 0.0)),
                            'trailing_stop_active': row.get('trailing_stop_active', 'False') == 'True',
                            'half_exit_done': row.get('half_exit_done', 'False') == 'True',
                            'original_size': float(row.get('original_size', float(row['size']))),
                        }
                        self.trades.append(trade_data)
                        self.total_trades = len(self.trades)
                        
                        # Restore open positions
                        if trade_data['status'] == 'open' and symbol in self.symbol_data:
                            if trade_data['action'] == 'long':
                                self.symbol_data[symbol]['long_position'] = trade_data
                            elif trade_data['action'] == 'short':
                                self.symbol_data[symbol]['short_position'] = trade_data
            except Exception as e:
                print(f"[INIT] Error loading trades from CSV: {e}")

        # Start background threads
        threading.Thread(target=self.strategy_loop, daemon=True).start()
        threading.Thread(target=self.monitor_positions, daemon=True).start()
        threading.Thread(target=self.process_message_queue, daemon=True).start()
    
    def set_telegram_app(self, app):
        """Set the telegram application after it's created"""
        self.telegram_app = app

    def get_main_keyboard(self):
        """Create the main button keyboard"""
        keyboard = [
            [KeyboardButton("📊 Status"), KeyboardButton("💰 Balance")],
            [KeyboardButton("📋 Positions"), KeyboardButton("📜 Recent History")],
            [KeyboardButton("📈 Total History")],
            [KeyboardButton("📈 Open Long"), KeyboardButton("📉 Open Short")],
            [KeyboardButton("Half Close Long"), KeyboardButton("Half Close Short")],
            [KeyboardButton("🔴 Close Long"), KeyboardButton("🔴 Close Short")],
            [KeyboardButton("🛑 Close All Positions")],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    def get_symbol_keyboard(self, action_type, filter_symbols=None):
        """Create inline keyboard for symbol selection
        filter_symbols: if provided, only show these symbols (e.g., only symbols with open positions)
        """
        symbols_to_show = filter_symbols if filter_symbols else TRADING_SYMBOLS
        keyboard = []
        row = []
        for i, symbol in enumerate(symbols_to_show):
            row.append(InlineKeyboardButton(symbol, callback_data=f"{action_type}_{symbol}"))
            if len(row) == 2 or i == len(symbols_to_show) - 1:
                keyboard.append(row)
                row = []
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
        return InlineKeyboardMarkup(keyboard)

    # ========== Telegram Message System ==========
    def send_telegram_message(self, message):
        """Queue a message to be sent via Telegram - always uses authorized chat_id"""
        if not self.telegram_app:
            print(f"[TELEGRAM] App not initialized: {message}")
            return
        
        # Always use authorized chat_id for security
        authorized_chat_id = AUTHORIZED_CHAT_ID
            
        self.message_queue.put({
            'message': message,
            'chat_id': authorized_chat_id
        })
    
    def process_message_queue(self):
        """Background thread to process message queue and send Telegram messages"""
        print("[TELEGRAM] Starting message queue processor...")
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while self.running:
            try:
                # Wait for a message in the queue (with timeout to check if still running)
                try:
                    msg_data = self.message_queue.get(timeout=1)
                except:
                    continue  # Timeout, check if still running
                
                if not self.telegram_app:
                    print(f"[TELEGRAM] App not initialized, skipping message")
                    continue
                
                message = msg_data['message']
                chat_id = msg_data['chat_id']
                
                # Send the message using the thread's event loop
                try:
                    loop.run_until_complete(self.send_telegram_message_async(message, chat_id))
                except Exception as e:
                    print(f"[TELEGRAM] Error sending message: {e}")
                    
            except Exception as e:
                print(f"[TELEGRAM] Error in message queue processor: {e}")
                time.sleep(1)
        
        loop.close()

    async def send_telegram_message_async(self, message, chat_id=AUTHORIZED_CHAT_ID):
        """Async method to send Telegram message - always uses authorized chat_id"""
        try:
            # Always use authorized chat_id for security
            authorized_chat_id = AUTHORIZED_CHAT_ID
            await self.telegram_app.bot.send_message(
                chat_id=authorized_chat_id,
                text=message,
                reply_markup=self.get_main_keyboard(),
                parse_mode='Markdown'
            )
            print(f"[TELEGRAM] Message sent to authorized chat {authorized_chat_id}")
        except Exception as e:
            print(f"[TELEGRAM ASYNC] Error sending message: {e}")
    
    def is_authorized(self, chat_id):
        """Check if the chat_id is authorized"""
        return str(chat_id) == AUTHORIZED_CHAT_ID

    def get_status_message(self):
        """Generate comprehensive status message for all symbols"""
        position_info = "📊 *Open Positions:*\n"
        has_positions = False
        
        for symbol in TRADING_SYMBOLS:
            data = self.symbol_data[symbol]
            long_pos = data['long_position']
            short_pos = data['short_position']
            
            if long_pos or short_pos:
                has_positions = True
                position_info += f"\n*{symbol}:*\n"
                if long_pos:
                    position_info += f"📈 LONG: ${long_pos['entry_price']:.4f} | SL: ${long_pos['stop_loss']:.4f} | TP: ${long_pos['take_profit']:.4f}\n"
                if short_pos:
                    position_info += f"📉 SHORT: ${short_pos['entry_price']:.4f} | SL: ${short_pos['stop_loss']:.4f} | TP: ${short_pos['take_profit']:.4f}\n"
        
        if not has_positions:
            position_info = "📊 *No open positions*\n"
        
        # Indicators for all symbols
        indicators_info = "\n📊 *Current Indicators:*\n"
        for symbol in TRADING_SYMBOLS:
            data = self.symbol_data[symbol]
            price = data['current_price']
            price_msg = f"${price:.4f}" if price else "N/A"
            indicators_info += f"\n*{symbol}:*\n"
            indicators_info += f"• Price: {price_msg}\n"
            indicators_info += f"• ATR: {data['current_atr_value']:.4f}\n"
            indicators_info += f"• Volume Osc: {data['current_vol_os']:.4f}%\n"
            indicators_info += f"• RSI: {data['current_rsi_value']:.4f}\n"
            if data['fs']:
                indicators_info += f"• FS: {data['fs'][-1]:.4f}\n"
            if data['tr']:
                indicators_info += f"• TR: {data['tr'][-1]:.4f}\n"
        
        # Performance
        closed_trades = [t for t in self.trades if t['status'] == 'closed']
        total_pnl = sum(trade['pnl'] for trade in closed_trades)
        win_trades = len([t for t in closed_trades if t['pnl'] > 0])
        win_rate = (win_trades / len(closed_trades)) * 100 if closed_trades else 0
        
        performance_info = f"""
💰 *Performance:*
• Balance: `${self.balance:.4f}`
• Total PNL: `${total_pnl:.4f}`
• Total Trades: `{len(closed_trades)}`
• Win Rate: `{win_rate:.1f}%`
• Open Positions: `{len([t for t in self.trades if t['status'] == 'open'])}`
        """
        
        return f"🤖 *MULTI-SYMBOL TRADING BOT STATUS*\n\n{position_info}{indicators_info}{performance_info}"

    def get_recent_history(self):
        """Get last 3 closed positions"""
        closed_trades = [t for t in self.trades if t['status'] == 'closed']
        recent_trades = sorted(closed_trades, key=lambda x: x['exit_time'], reverse=True)[:3]
        
        if not recent_trades:
            return "📜 *Recent History:*\nNo closed positions yet."
        
        history_msg = "📜 *Last 3 Closed Positions:*\n\n"
        
        for i, trade in enumerate(recent_trades, 1):
            symbol = trade.get('symbol', 'N/A')
            pnl_percent = (trade['pnl'] / (trade['entry_price'] * trade['size'] / LEVERAGE)) * 100 if trade['entry_price'] * trade['size'] > 0 else 0
            
            history_msg += f"""
*Trade {i} ({symbol}):*
• Action: `{trade['action'].upper()}`
• Entry: `${trade['entry_price']:.4f}`
• Exit: `${trade['exit_price']:.4f}`
• PNL: `${trade['pnl']:.4f}` ({pnl_percent:+.2f}%)
• Size: `{trade['size']:.4f}`
• Reason: `{trade['reason']}`
• Date: `{trade['exit_time'][:19]} UTC`
────────────────────
"""
        
        return history_msg

    def get_total_history(self):
        """Get all closed positions with summary"""
        closed_trades = [t for t in self.trades if t['status'] == 'closed']
        
        if not closed_trades:
            return "📈 *Total History:*\nNo closed positions yet."
        
        # Sort by exit time
        closed_trades = sorted(closed_trades, key=lambda x: x['exit_time'], reverse=True)
        
        total_pnl = sum(trade['pnl'] for trade in closed_trades)
        win_trades = len([t for t in closed_trades if t['pnl'] > 0])
        loss_trades = len([t for t in closed_trades if t['pnl'] < 0])
        win_rate = (win_trades / len(closed_trades)) * 100
        
        best_trade = max(closed_trades, key=lambda x: x['pnl']) if closed_trades else None
        worst_trade = min(closed_trades, key=lambda x: x['pnl']) if closed_trades else None
        
        history_msg = f"""📈 *Total Trading History:*

📊 *Summary:*
• Total Trades: `{len(closed_trades)}`
• Winning Trades: `{win_trades}`
• Losing Trades: `{loss_trades}`
• Win Rate: `{win_rate:.1f}%`
• Total PNL: `${total_pnl:.4f}`
• ROI: `{(total_pnl/self.initial_balance)*100:.4f}%`

"""
        
        if best_trade:
            best_pnl_percent = (best_trade['pnl'] / (best_trade['entry_price'] * best_trade['size'] / LEVERAGE)) * 100
            history_msg += f"🏆 *Best Trade:* `${best_trade['pnl']:.4f}` ({best_pnl_percent:+.2f}%)\n"
        
        if worst_trade:
            worst_pnl_percent = (worst_trade['pnl'] / (worst_trade['entry_price'] * worst_trade['size'] / LEVERAGE)) * 100
            history_msg += f"📉 *Worst Trade:* `${worst_trade['pnl']:.4f}` ({worst_pnl_percent:+.2f}%)\n"
        
        history_msg += f"\n*Recent Trades:*\n"
        
        # Show last 5 trades in detail
        for i, trade in enumerate(closed_trades[:5], 1):
            pnl_percent = (trade['pnl'] / (trade['entry_price'] * trade['size'] / LEVERAGE)) * 100
            emoji = "🟢" if trade['pnl'] > 0 else "🔴" if trade['pnl'] < 0 else "⚪"
            
            history_msg += f"""
{emoji} *{trade['action'].upper()}* | PNL: `${trade['pnl']:.4f}` ({pnl_percent:+.2f}%)
   Entry: `${trade['entry_price']:.4f}` | Exit: `${trade['exit_price']:.4f}`
   Reason: `{trade['reason']}` | Date: `{trade['exit_time'][:19]}`
"""
        
        if len(closed_trades) > 5:
            history_msg += f"\n... and {len(closed_trades) - 5} more trades"
        
        return history_msg

    # ========== Telegram Command Handlers ==========
    async def handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        chat_id = str(update.effective_chat.id)
        
        if not self.is_authorized(chat_id):
            await update.message.reply_text("❌ Unauthorized access. This bot is restricted to authorized users only.")
            return
        
        welcome_text = """
🤖 Welcome to the Trading Bot!

Use the buttons below to control the bot or type /help for more information.

✅ You are authorized to use this bot.
        """
        await update.message.reply_text(
            welcome_text,
            reply_markup=self.get_main_keyboard()
        )

    async def handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command with buttons"""
        chat_id = str(update.effective_chat.id)
        
        if not self.is_authorized(chat_id):
            await update.message.reply_text("❌ Unauthorized access. This bot is restricted to authorized users only.")
            return
        
        help_text = """
🤖 *Trading Bot Commands:*

*Basic Commands:*
• 📊 Status - Show current status and indicators
• 💰 Balance - Show account balance and performance
• 📋 Positions - Show open positions

*Trading Commands:*
• 📈 Open Long - Force open long position
• 📉 Open Short - Force open short position  
• 🔴 Close Long - Force close long position
• 🔴 Close Short - Force close short position
• 🛑 Close All Positions - Fetch and close all open positions from Bitget

*History Commands:*
• 📜 Recent History - Show last 3 closed positions
• 📈 Total History - Show all trading history with stats

*Auto Features:*
• Strategy runs every hour
• Real-time position monitoring
• Automatic stop-loss/take-profit
• Telegram notifications for all trades

✅ *Authorized User Access Only*

*Use the buttons below to control the bot:*
        """
        
        await update.message.reply_text(
            help_text, 
            parse_mode='Markdown', 
            reply_markup=self.get_main_keyboard()
        )

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button presses and text messages - AUTHORIZED USERS ONLY"""
        chat_id = str(update.effective_chat.id)
        
        # Check authorization first
        if not self.is_authorized(chat_id):
            await update.message.reply_text("❌ Unauthorized access. This bot is restricted to authorized users only.")
            return
        
        message_text = update.message.text

        if message_text == "📊 Status":
            status_msg = self.get_status_message()
            await update.message.reply_text(
                status_msg,
                reply_markup=self.get_main_keyboard()
            )
            
        elif message_text == "💰 Balance":
            self.balance = self.fetch_real_balance()
            closed_trades = [t for t in self.trades if t['status'] == 'closed']
            total_pnl = sum(trade['pnl'] for trade in closed_trades)
            win_trades = len([t for t in closed_trades if t['pnl'] > 0])
            win_rate = (win_trades / len(closed_trades)) * 100 if closed_trades else 0
            balance_msg = f"""
💼 *Account Balance:*
• Current Balance: `${self.balance:.4f}`
• Initial Balance: `${self.initial_balance:.4f}`
• Total PNL: `${total_pnl:.4f}`
• ROI: `{(total_pnl/self.initial_balance)*100:.4f}%`

📈 *Performance:*
• Total Trades: `{len(closed_trades)}`
• Winning Trades: `{win_trades}`
• Win Rate: `{win_rate:.1f}%`
• Open Positions: `{len([t for t in self.trades if t['status'] == 'open'])}`
            """
            await update.message.reply_text(
                balance_msg, 
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
            
        elif message_text == "📈 Open Long":
            await update.message.reply_text(
                "📈 *Select symbol for LONG position:*",
                reply_markup=self.get_symbol_keyboard("open_long"),
                parse_mode='Markdown'
            )
            
        elif message_text == "📉 Open Short":
            await update.message.reply_text(
                "📉 *Select symbol for SHORT position:*",
                reply_markup=self.get_symbol_keyboard("open_short"),
                parse_mode='Markdown'
            )
            
        elif message_text == "🔴 Close Long":
            # Check if there are any open long positions
            open_symbols = []
            for symbol in TRADING_SYMBOLS:
                if self.symbol_data[symbol]['long_position']:
                    open_symbols.append(symbol)
            
            if not open_symbols:
                await update.message.reply_text(
                    "❌ No LONG positions to close",
                    reply_markup=self.get_main_keyboard()
                )
            elif len(open_symbols) == 1:
                # Only one position, close it directly
                await update.message.reply_text(
                    f"🔄 Closing {open_symbols[0]} LONG position...",
                    reply_markup=self.get_main_keyboard()
                )
                self.close_position('long', "manual_close", open_symbols[0])
            else:
                # Multiple positions, show selection - only show symbols with open positions
                await update.message.reply_text(
                    "🔴 *Select symbol to close LONG position:*",
                    reply_markup=self.get_symbol_keyboard("close_long", open_symbols),
                    parse_mode='Markdown'
                )
                
        elif message_text == "🔴 Close Short":
            # Check if there are any open short positions
            open_symbols = []
            for symbol in TRADING_SYMBOLS:
                if self.symbol_data[symbol]['short_position']:
                    open_symbols.append(symbol)
            
            if not open_symbols:
                await update.message.reply_text(
                    "❌ No SHORT positions to close",
                    reply_markup=self.get_main_keyboard()
                )
            elif len(open_symbols) == 1:
                # Only one position, close it directly
                await update.message.reply_text(
                    f"🔄 Closing {open_symbols[0]} SHORT position...",
                    reply_markup=self.get_main_keyboard()
                )
                self.close_position('short', "manual_close", open_symbols[0])
            else:
                # Multiple positions, show selection - only show symbols with open positions
                await update.message.reply_text(
                    "🔴 *Select symbol to close SHORT position:*",
                    reply_markup=self.get_symbol_keyboard("close_short", open_symbols),
                    parse_mode='Markdown'
                )
                
        elif message_text == "📋 Positions":
            positions_msg = "📋 *Current Positions:*\n\n"
            has_positions = False
            
            for symbol in TRADING_SYMBOLS:
                data = self.symbol_data[symbol]
                long_pos = data['long_position']
                short_pos = data['short_position']
                
                if long_pos or short_pos:
                    has_positions = True
                    positions_msg += f"*{symbol}:*\n"
                    
                    if long_pos:
                        price = data['current_price']
                        unrealized_pnl = (price - long_pos['entry_price']) * long_pos['size'] if price else 0
                        positions_msg += f"""
📈 *LONG Position:*
• Entry Price: `${long_pos['entry_price']:.4f}`
• Size: `{long_pos['size']:.4f}`
• Stop Loss: `${long_pos['stop_loss']:.4f}`
• Take Profit: `${long_pos['take_profit']:.4f}`
• Unrealized PNL: `${unrealized_pnl:.4f}`
• Trailing Stop: `{'Active' if long_pos.get('trailing_stop_active', False) else 'Inactive'}`
                        """
                    
                    if short_pos:
                        price = data['current_price']
                        unrealized_pnl = (short_pos['entry_price'] - price) * short_pos['size'] if price else 0
                        positions_msg += f"""
📉 *SHORT Position:*
• Entry Price: `${short_pos['entry_price']:.4f}`
• Size: `{short_pos['size']:.4f}`
• Stop Loss: `${short_pos['stop_loss']:.4f}`
• Take Profit: `${short_pos['take_profit']:.4f}`
• Unrealized PNL: `${unrealized_pnl:.4f}`
• Trailing Stop: `{'Active' if short_pos.get('trailing_stop_active', False) else 'Inactive'}`
                        """
                    positions_msg += "\n"
            
            if not has_positions:
                positions_msg = "📋 *No open positions*\n"
                
            await update.message.reply_text(
                positions_msg, 
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
            
        elif message_text == "📜 Recent History":
            recent_history = self.get_recent_history()
            await update.message.reply_text(
                recent_history,
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
            
        elif message_text == "📈 Total History":
            total_history = self.get_total_history()
            await update.message.reply_text(
                total_history,
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
        elif message_text == "Half Close Long":
            # Check if there are any open long positions
            open_symbols = []
            for symbol in TRADING_SYMBOLS:
                if self.symbol_data[symbol]['long_position']:
                    open_symbols.append(symbol)
            
            if not open_symbols:
                await update.message.reply_text(
                    "❌ No LONG positions to half-close",
                    reply_markup=self.get_main_keyboard()
                )
            elif len(open_symbols) == 1:
                # Only one position, half close it directly
                await update.message.reply_text(
                    f"Halving {open_symbols[0]} LONG position…",
                    reply_markup=self.get_main_keyboard()
                )
                self.close_half_position('long', open_symbols[0])
            else:
                # Multiple positions, show selection - only show symbols with open positions
                await update.message.reply_text(
                    "🔵 *Select symbol to half-close LONG position:*",
                    reply_markup=self.get_symbol_keyboard("half_close_long", open_symbols),
                    parse_mode='Markdown'
                )
        elif message_text == "Half Close Short":
            # Check if there are any open short positions
            open_symbols = []
            for symbol in TRADING_SYMBOLS:
                if self.symbol_data[symbol]['short_position']:
                    open_symbols.append(symbol)
            
            if not open_symbols:
                await update.message.reply_text(
                    "❌ No SHORT positions to half-close",
                    reply_markup=self.get_main_keyboard()
                )
            elif len(open_symbols) == 1:
                # Only one position, half close it directly
                await update.message.reply_text(
                    f"Halving {open_symbols[0]} SHORT position…",
                    reply_markup=self.get_main_keyboard()
                )
                self.close_half_position('short', open_symbols[0])
            else:
                # Multiple positions, show selection - only show symbols with open positions
                await update.message.reply_text(
                    "🔵 *Select symbol to half-close SHORT position:*",
                    reply_markup=self.get_symbol_keyboard("half_close_short", open_symbols),
                    parse_mode='Markdown'
                )
        elif message_text == "🛑 Close All Positions":
            await update.message.reply_text(
                "🔄 Fetching all open positions from Bitget...",
                reply_markup=self.get_main_keyboard()
            )
            self.close_all_positions()
        else:
            await update.message.reply_text(
                "❌ Unknown command. Use the buttons below or type /help for available commands.",
                reply_markup=self.get_main_keyboard()
            )
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback queries from inline keyboard buttons"""
        query = update.callback_query
        await query.answer()
        
        chat_id = str(query.from_user.id)
        
        # Check authorization
        if not self.is_authorized(chat_id):
            await query.edit_message_text("❌ Unauthorized access. This bot is restricted to authorized users only.")
            return
        
        callback_data = query.data
        
        if callback_data == "cancel":
            await query.edit_message_text("❌ Operation cancelled.", reply_markup=None)
            return
        
        # Parse callback data format: action_direction_symbol or action_symbol
        # Examples: "open_long_ETHUSDT", "close_short_SOLUSDT", "half_close_long_TAOUSDT"
        parts = callback_data.split("_")
        
        if len(parts) < 2:
            await query.edit_message_text("❌ Invalid selection.", reply_markup=None)
            return
        
        action = parts[0]
        symbol = parts[-1]  # Last part is always the symbol
        
        if symbol not in TRADING_SYMBOLS:
            await query.edit_message_text(f"❌ Invalid symbol: {symbol}", reply_markup=None)
            return
        
        # Execute the action based on callback data
        if action == "open":
            # open_long_SYMBOL or open_short_SYMBOL
            direction = parts[1]  # "long" or "short"
            await query.edit_message_text(
                f"🔄 Opening {symbol} {direction.upper()} position...",
                reply_markup=None
            )
            self.open_position(direction, symbol)
            
        elif action == "close":
            # close_long_SYMBOL or close_short_SYMBOL
            direction = parts[1]  # "long" or "short"
            await query.edit_message_text(
                f"🔄 Closing {symbol} {direction.upper()} position...",
                reply_markup=None
            )
            self.close_position(direction, "manual_close", symbol)
            
        elif action == "half":
            # half_close_long_SYMBOL or half_close_short_SYMBOL
            direction = parts[2]  # "long" or "short"
            await query.edit_message_text(
                f"Halving {symbol} {direction.upper()} position…",
                reply_markup=None
            )
            self.close_half_position(direction, symbol)
            
        else:
            await query.edit_message_text("❌ Unknown action.", reply_markup=None)

    def fetch_real_balance(self):
        """Fetch actual USDT available balance from Bitget Futures account"""
        try:
            params = {
                "productType": PRODUCT_TYPE
            }
            response = self.maxAccountApi.accounts(params)

            if response.get('code') != '00000':
                print(f"[BALANCE 🔴] API Error: {response.get('msg')}")
                return None

            accounts = response.get('data', [])

            for acc in accounts:
                if acc.get('marginCoin') == 'USDT':
                    available = float(acc.get('available', 0))
                    print(f"[BALANCE 🟢] Fetched real balance: ${available:.4f} USDT")
                    return available

            print("[BALANCE 🔴] USDT account not found")
            return None

        except Exception as e:
            print(f"[BALANCE 🔴] Error fetching balance: {e}")
            return None

    def set_leverage(self, leverage=10, symbol=None):
        """Set leverage for a symbol in USDT-FUTURES"""
        if symbol is None:
            return
        try:
            params = {
                "symbol": symbol,
                "productType": PRODUCT_TYPE,
                "marginCoin": MARGIN_COIN,
                "leverage": str(leverage)
            }
            response = self.maxAccountApi.setLeverage(params)
            if response.get('code') == '00000':
                print(f"[LEVERAGE 🟢] {symbol} successfully set to {leverage}x")
            else:
                print(f"[LEVERAGE 🔴] {symbol} failed: {response.get('msg')}")
        except Exception as e:
            print(f"[LEVERAGE 🔴] {symbol} error setting leverage: {e}")

    # ========== Data Fetching & Indicators ==========
    def get_current_price(self, symbol=None):
        if symbol is None:
            return
        try:
            params = {
                "symbol": symbol,
                "productType": PRODUCT_TYPE,
            }
            response = self.maxMarketApi.tickers(params)
            
            if response['code'] != '00000':
                print(f"[REST] {symbol} API Error: {response['msg']}")
                return None
                
            for ticker in response['data']:
                if ticker['symbol'] == symbol:
                    price = float(ticker['lastPr'])
                    self.symbol_data[symbol]['current_price'] = round(price, 4)
                    return round(price, 4)
                    
            print(f"[REST] {symbol} symbol not found in response")
            return None
            
        except Exception as e:
            print(f"[REST] {symbol} price fetch error: {e}")
            return None

    def fetch_candles(self, interval: str, limit: int, symbol=None):
        if symbol is None:
            return
        params = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "granularity": interval,
            "limit": limit
        }
        try:
            response = self.maxMarketApi.history(params)
            
            if response['code'] != '00000':
                print(f"[DATA] {symbol} API Error: {response['msg']}")
                return None
                
            data = response.get("data", [])
            candles = []
            for entry in data:
                candle = {
                    "timestamp": datetime.fromtimestamp(int(entry[0]) / 1000, timezone.utc),
                    "open": float(entry[1]),
                    "high": float(entry[2]),
                    "low": float(entry[3]),
                    "close": float(entry[4]),
                    "volume": float(entry[5]),
                    "quote_volume": float(entry[6])
                }
                candles.append(candle)
            print(f"[DATA] {symbol} Fetched {len(candles)} {interval} candles")
            return candles
        except Exception as e:
            print(f"[DATA] {symbol} Error fetching candles: {e}")
            return None
        
    def calculate_atr(self, df, atr_length=14):
        high = df['high']
        low = df['low']
        close = df['close']
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        # Wilder's moving average (RMA) uses ewm with alpha = 1/period
        atr = tr.ewm(alpha=1/atr_length, adjust=False, min_periods=atr_length).mean()
        df['atr'] = atr.fillna(0)
        return df
    
    def calculate_volume_oscillator(self, df):
        df = pd.DataFrame(df)
        vol_ema5 = df['volume'].ewm(span=5, adjust=False).mean()
        vol_ema10 = df['volume'].ewm(span=10, adjust=False).mean()
        vol_osc = (vol_ema5 - vol_ema10) / vol_ema10 * 100
        df['vol_os'] = vol_osc.fillna(0)
        return df
    
    # ========== RSI Calculation ==========
    def calculate_rsi(self, df, rsi_length=RSI_LENGTH):
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/rsi_length, adjust=False, min_periods=rsi_length).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/rsi_length, adjust=False, min_periods=rsi_length).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        df['rsi'] = df['rsi'].fillna(50)
        return df
    
    def calculate_fs(self, df, fs_length=FS_LENGTH):
        high = df['high'].values
        low = df['low'].values
        median = (high + low) / 2
        value = np.zeros(len(median))
        fs = np.zeros(len(median))
        tr = np.zeros(len(median))
        
        # Initialize first values
        value[0] = 0
        fs[0] = 0
        tr[0] = 0
        
        for i in range(1, len(median)):
            if i < fs_length:
                # Not enough data for full window, use simple calculation
                window = median[:i+1]
                max_h = np.max(window)
                min_l = np.min(window)
            else:
                window = median[i-fs_length+1:i+1]
                max_h = np.max(window)
                min_l = np.min(window)
            
            if max_h != min_l:
                val = 0.33 * 2 * ((median[i] - min_l)/(max_h - min_l) - 0.5) + 0.67 * value[i-1]
                val = np.clip(val, -0.999, 0.999)
                value[i] = val
                fs[i] = 0.5 * np.log((1 + val)/(1 - val)) + 0.5 * fs[i-1]
                tr[i] = fs[i-1]
            else:
                value[i] = 0
                fs[i] = fs[i-1]
                tr[i] = tr[i-1]
        
        df['fs'] = fs
        df['tr'] = tr
        return df
    
    def update_trailing_stop(self, current_price, position):
        if not position:
            return
        
        symbol = position.get('symbol')
        config = SYMBOL_CONFIG[symbol]
        data = self.symbol_data[symbol]
        take_profit = position['take_profit']

        if 'max_profit_price' not in position:
            position['max_profit_price'] = position['entry_price']
            position['trailing_stop_active'] = False

        changed = False  # track if we update anything

        if position['action'] == 'long':
            if current_price > position['max_profit_price']:
                position['max_profit_price'] = current_price
                changed = True
                print(f"[TRAILING STOP] {symbol} New max profit price for LONG: ${position['max_profit_price']:.4f}")

            if not position.get('half_exit_done', False) and current_price >= take_profit:
                self.execute_half_exit(position, current_price, symbol)
                position['trailing_stop_active'] = True
                changed = True

            if position['trailing_stop_active']:
                trailing_stop_price = position['max_profit_price'] - (data['current_atr_value'] * config['long_second_sl_level'])
                if trailing_stop_price > position['stop_loss']:
                    position['stop_loss'] = trailing_stop_price
                    changed = True
                    print(f"[TRAILING STOP] {symbol} Updated stop loss for LONG: ${position['stop_loss']:.4f}")

        else:  # short
            if current_price < position['max_profit_price']:
                position['max_profit_price'] = current_price
                changed = True
                print(f"[TRAILING STOP] {symbol} New max profit price for SHORT: ${position['max_profit_price']:.4f}")

            if not position.get('half_exit_done', False) and current_price <= take_profit:
                self.execute_half_exit(position, current_price, symbol)
                position['trailing_stop_active'] = True
                changed = True

            if position['trailing_stop_active']:
                trailing_stop_price = position['max_profit_price'] + (data['current_atr_value'] * config['short_second_sl_level'])
                if trailing_stop_price < position['stop_loss']:
                    position['stop_loss'] = trailing_stop_price
                    changed = True
                    print(f"[TRAILING STOP] {symbol} Updated stop loss for SHORT: ${position['stop_loss']:.4f}")

        if changed:
            self.save_trades()

    def execute_half_exit(self, position, current_price, symbol=None):
        """Execute half position exit and update position size"""
        if symbol is None:
            symbol = position.get('symbol')
        with self.trade_lock:
            if position.get('half_exit_done', False):
                print(f"[HALF EXIT] {symbol} Half exit already executed for {position['action'].upper()} position")
                return

            if 'original_size' not in position:
                position['original_size'] = position['size']

            half_size = round(position['size'] / 2, 2)
            remaining_size = position['size'] - half_size

            if position['action'] == 'long':
                pnl = (current_price - position['entry_price']) * half_size
            else:
                pnl = (position['entry_price'] - current_price) * half_size

            fee = (current_price + position['entry_price']) * FEE_PERCENT * half_size
            net_pnl = pnl - fee

            half_trade = {
                'symbol': symbol,
                'entry_time': position['entry_time'],
                'exit_time': datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                'action': position['action'],
                'entry_price': position['entry_price'],
                'exit_price': current_price,
                'size': half_size,
                'status': 'closed',
                'pnl': net_pnl,
                'fee': fee,
                'ideal_pnl': pnl,
                'reason': 'half_exit_take_profit',
                'stop_loss': position['stop_loss'],
                'take_profit': position['take_profit'],
                'max_profit_price': position.get('max_profit_price', position['entry_price']),
                'trailing_stop_active': False,
                'half_exit_done': True,
                'original_size': position['original_size']
            }

            # Send order first and check if it succeeds
            order_success, order_error = self.send_order(f'exit_{position["action"]}', current_price, half_size, symbol)
            
            if not order_success:
                error_msg = f"❌ *FAILED TO HALF EXIT {symbol} {position['action'].upper()} POSITION*\n"
                error_msg += f"Exit Price: `${current_price:.4f}`\n"
                error_msg += f"Half Size: `{half_size:.4f}`\n"
                error_msg += f"Error: `{order_error}`"
                self.send_telegram_message(error_msg)
                print(f"[HALF EXIT] 🔴 {symbol} Failed to half exit {position['action'].upper()} position - {order_error}")
                return
            
            self.balance = self.fetch_real_balance()

            # Order succeeded, update position and save
            position['size'] = remaining_size
            position['half_exit_done'] = True
            position['trailing_stop_active'] = True

            self.trades.append(half_trade)
            self.save_trades()

            self.send_telegram_message(f"[HALF EXIT] 🔵 {symbol} Executed half exit for {position['action'].upper()} position | "
                  f"Exit Price: `${current_price:.4f}`\n"
                  f"Half Size: `{half_size:.4f}`\n"
                  f"Remaining Size: `{remaining_size:.4f}`\n"
                  f"PNL: ${net_pnl:.4f}"
                  )
            
            print(f"[HALF EXIT] 🔵 {symbol} Executed half exit for {position['action'].upper()} position | "
                  f"Exit Price: ${current_price:.4f} | "
                  f"Half Size: {half_size:.4f} | "
                  f"Remaining Size: {remaining_size:.4f} | "
                  f"PNL: ${net_pnl:.4f}")

    def update_indicators(self, symbol=None):
        if symbol is None:
            # Update all symbols
            for sym in TRADING_SYMBOLS:
                self.update_indicators(sym)
            return
            
        new_candles = self.fetch_candles(INTERVAL, 200, symbol)
        if not new_candles:
            print(f"[DATA] {symbol} No new candles fetched. Using existing data.")
            return
            
        candles = sorted(new_candles, key=lambda x: x['timestamp'])
        print(f"[INDICATORS] {symbol} Updating with {len(candles)} candles")

        df = pd.DataFrame(candles)

        df = self.calculate_fs(df, FS_LENGTH)
        df = self.calculate_volume_oscillator(df)
        df = self.calculate_atr(df, ATR_LENGTH)
        df = self.calculate_rsi(df, RSI_LENGTH)

        self.symbol_data[symbol]['candles'] = candles
        self.symbol_data[symbol]['fs'] = df['fs'].tolist()
        self.symbol_data[symbol]['tr'] = df['tr'].tolist()
        self.symbol_data[symbol]['current_vol_os'] = df['vol_os'].iloc[-1]
        self.symbol_data[symbol]['current_atr_value'] = df['atr'].iloc[-1]
        self.symbol_data[symbol]['current_rsi_value'] = df['rsi'].iloc[-1]

        print(f"[INDICATORS] {symbol} Updated FS: {self.symbol_data[symbol]['fs'][-1]:.4f}, "
            f"TR: {self.symbol_data[symbol]['tr'][-1]:.4f}, "
            f"VOL_OS: {self.symbol_data[symbol]['current_vol_os']:.4f}, "
            f"ATR_VAL: {self.symbol_data[symbol]['current_atr_value']:.4f}, "
            f"RSI_VALUE: {self.symbol_data[symbol]['current_rsi_value']:.4f}")

    # ========== Trading Logic ==========
    def evaluate_condition(self, value, operator, threshold):
        """Evaluate a condition with the given operator"""
        if operator == '>':
            return value > threshold
        elif operator == '<':
            return value < threshold
        elif operator == '>=':
            return value >= threshold
        elif operator == '<=':
            return value <= threshold
        elif operator == '==':
            return value == threshold
        elif operator == '!=':
            return value != threshold
        else:
            print(f"[SIGNAL] Unknown operator: {operator}, defaulting to >")
            return value > threshold
    
    def check_signal(self, current_time: str, symbol=None):
        if symbol is None:
            # Check signals for all symbols
            for sym in TRADING_SYMBOLS:
                self.check_signal(current_time, sym)
            return
            
        data = self.symbol_data[symbol]
        if len(data['fs']) < 3:
            print(f"[SIGNAL] {symbol} Not enough data to check signals")
            return
        
        fs_now = data['fs'][-1]
        tr_now = data['tr'][-1]
        
        fs_prev = data['fs'][-2]
        tr_prev = data['tr'][-2]

        fs_cross_up = (fs_prev < tr_prev) and (fs_now > tr_now)
        fs_cross_down = (fs_prev > tr_prev) and (fs_now < tr_now)

        current_price = self.get_current_price(symbol)
        if current_price is None:
            print(f"[SIGNAL] {symbol} Cannot get current price, skipping signal check")
            return

        if fs_cross_up:
            print(f"{symbol} UP cross is detected!")
        if fs_cross_down:
            print(f"{symbol} DOWN cross is detected!")
        
        config = SYMBOL_CONFIG[symbol]
        
        if fs_cross_up:
            # Long entry conditions with symbol-specific operators
            rsi_op, fs_op, vol_op = config['long_operators']
            rsi_value = abs(data['current_rsi_value'] - 50)
            fs_value = max(abs(tr_now), abs(fs_now))
            
            cond_entry = self.evaluate_condition(rsi_value, rsi_op, config['long_rsi_entry_level'])
            cond_entry_fs = self.evaluate_condition(fs_value, fs_op, config['long_fs_entry_level'])
            cond_vol = self.evaluate_condition(data['current_vol_os'], vol_op, 0)
            
            print(f"[SIGNAL] {symbol} LONG Conditions - RSI: {cond_entry}, FS: {cond_entry_fs}, VOL: {cond_vol}")
            
            if cond_entry and cond_entry_fs and cond_vol:
                long_pos = data['long_position']
                if long_pos:
                    if current_price < long_pos['entry_price']:
                        self.close_position('long', "Replace", symbol)
                        self.open_position('long', symbol)
                    else:
                        pass
                else:
                    self.open_position('long', symbol)
                    
        elif fs_cross_down:
            # Short entry conditions with symbol-specific operators
            rsi_op, fs_op, vol_op = config['short_operators']
            rsi_value = abs(data['current_rsi_value'] - 50)
            fs_value = max(abs(tr_now), abs(fs_now))
            
            cond_entry = self.evaluate_condition(rsi_value, rsi_op, config['short_rsi_entry_level'])
            cond_entry_fs = self.evaluate_condition(fs_value, fs_op, config['short_fs_entry_level'])
            cond_vol = self.evaluate_condition(data['current_vol_os'], vol_op, 0)
            
            print(f"[SIGNAL] {symbol} SHORT Conditions - RSI: {cond_entry}, FS: {cond_entry_fs}, VOL: {cond_vol}")
            
            if cond_entry and cond_entry_fs and cond_vol:
                short_pos = data['short_position']
                if short_pos:
                    if current_price > short_pos['entry_price']:
                        self.close_position('short', "Replace", symbol)
                        self.open_position('short', symbol)
                    else:
                        pass
                else:
                    self.open_position('short', symbol)

    def open_position(self, direction, symbol=None):
        if symbol is None:
            return
            
        with self.trade_lock:
            # Check total number of open positions across all symbols
            total_open_positions = 0
            for sym in TRADING_SYMBOLS:
                sym_data = self.symbol_data[sym]
                if sym_data['long_position']:
                    total_open_positions += 1
                if sym_data['short_position']:
                    total_open_positions += 1
            
            # Check if we're at the maximum limit
            if total_open_positions >= MAX_OPEN_POSITIONS:
                print(f"[TRADE] Cannot open {symbol} {direction.upper()}: Maximum open positions ({MAX_OPEN_POSITIONS}) reached. Current: {total_open_positions}")
                self.send_telegram_message(
                    f"❌ *CANNOT OPEN {symbol} {direction.upper()} POSITION*\n"
                    f"Maximum open positions limit reached: `{MAX_OPEN_POSITIONS}`\n"
                    f"Current open positions: `{total_open_positions}`"
                )
                return
            
            data = self.symbol_data[symbol]
            config = SYMBOL_CONFIG[symbol]
            
            # Check if position already exists for this symbol
            if direction == 'long' and data['long_position']:
                print(f"[TRADE] {symbol} Cannot open LONG: an open LONG position already exists.")
                self.send_telegram_message(f"❌ {symbol} Cannot open LONG: Position already exists")
                return
            if direction == 'short' and data['short_position']:
                print(f"[TRADE] {symbol} Cannot open SHORT: an open SHORT position already exists.")
                self.send_telegram_message(f"❌ {symbol} Cannot open SHORT: Position already exists")
                return
        
            price = self.get_current_price(symbol)
            if price is None:
                print(f"[TRADE] {symbol} Failed to fetch current price for open position.")
                self.send_telegram_message(f"❌ {symbol} Failed to fetch price for opening position")
                return

            self.balance = self.fetch_real_balance()
            # Only update balance if there are no open positions
            # If there are open positions, use the existing balance

            size = round((self.balance * (1.0/(5-total_open_positions)) * LEVERAGE) / price, 2)

            current_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

            if direction == 'long':
                stop_loss_level = config['long_stop_loss_level']
                take_profit_level = config['long_take_profit_level']
            else:
                stop_loss_level = config['short_stop_loss_level']
                take_profit_level = config['short_take_profit_level']
            
            initial_risk = data['current_atr_value'] * stop_loss_level
            initial_profit = data['current_atr_value'] * take_profit_level

            if direction == 'long':
                stop_loss = price - initial_risk
                take_profit = price + initial_profit 

            else:
                stop_loss = price + initial_risk
                take_profit = price - initial_profit

            # Send order first and check if it succeeds
            order_success, order_error = self.send_order(direction, price, size, symbol)
            
            if not order_success:
                error_msg = f"❌ *FAILED TO OPEN {symbol} {direction.upper()} POSITION*\n"
                error_msg += f"Price: `${price:.4f}`\n"
                error_msg += f"Size: `{size:.4f}`\n"
                error_msg += f"Error: `{order_error}`"
                self.send_telegram_message(error_msg)
                print(f"[TRADE] 🔴 {symbol} Failed to open {direction.upper()} position - {order_error}")
                return
            
            self.balance = self.fetch_real_balance()
            
            # Order succeeded, create and save the position
            trade = {
                'symbol': symbol,
                'entry_time': current_time,
                'exit_time': None,
                'action': direction,
                'entry_price': price,
                'exit_price': None,
                'size': size,
                'status': 'open',
                'pnl': 0.0,
                'fee': 0.0,
                'ideal_pnl': 0.0,
                'reason': '',
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'max_profit_price': price,
                'trailing_stop_active': False,
                'half_exit_done': False,
                'original_size': size
            }

            if direction == 'long':
                data['long_position'] = trade
            else:
                data['short_position'] = trade

            self.trades.append(trade)
            self.save_trades()
            
            self.send_telegram_message(
                f"✅ *OPENED {symbol} {direction.upper()} POSITION*\n"
                f"Price: `${price:.4f}`\n"
                f"Size: `{size:.4f}`\n"
                f"SL: `${stop_loss:.4f}`\n"
                f"TP: `${take_profit:.4f}`\n"
                f"Leverage: `{LEVERAGE}x`"
            )
            print(f"[TRADE] 🟢 {symbol} Opened {direction.upper()} position at ${price:.4f}, size: {size:.4f}, SL: ${stop_loss:.4f}")

    def close_trade(self, trade, current_price, reason):
        symbol = trade.get('symbol')
        with self.trade_lock:
            trade['exit_time'] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            trade['exit_price'] = current_price
            
            if trade['action'] == 'long':
                trade['pnl'] = (current_price - trade['entry_price']) * trade['size']
            else:
                trade['pnl'] = (trade['entry_price'] - current_price) * trade['size']
                
            trade['fee'] = (current_price + trade['entry_price']) * FEE_PERCENT * trade['size']
            trade['ideal_pnl'] = trade['pnl']
            trade['pnl'] -= trade['fee']
            trade['status'] = 'closed'
            trade['reason'] = reason
            
            # Send order first and check if it succeeds
            order_success, order_error = self.send_order(f'exit_{trade["action"]}', current_price, trade['size'], symbol)
            
            if not order_success:
                error_msg = f"❌ *FAILED TO CLOSE {symbol} {trade['action'].upper()} POSITION*\n"
                error_msg += f"Entry: `${trade['entry_price']:.4f}`\n"
                error_msg += f"Exit Price: `${current_price:.4f}`\n"
                error_msg += f"Size: `{trade['size']:.4f}`\n"
                error_msg += f"Error: `{order_error}`"
                self.send_telegram_message(error_msg)
                print(f"[TRADE] 🔴 {symbol} Failed to close {trade['action'].upper()} position - {order_error}")
                # Don't update position status if order failed
                return

            self.balance = self.fetch_real_balance()
            
            # Order succeeded, finalize the close
            data = self.symbol_data[symbol]
            if trade['action'] == 'long':
                data['long_position'] = None
            else:
                data['short_position'] = None
            
            self.save_trades()

            pnl_percent = (trade['pnl'] / (trade['entry_price'] * trade['size'] / LEVERAGE)) * 100
            self.send_telegram_message(
                f"🔴 *CLOSED {symbol} {trade['action'].upper()} POSITION*\n"
                f"Entry: `${trade['entry_price']:.4f}`\n"
                f"Exit: `${current_price:.4f}`\n"
                f"PNL: `${trade['pnl']:.4f}` ({pnl_percent:+.2f}%)\n"
                f"Reason: `{reason}`"
            )
            
            print(f"[TRADE] 🔴 {symbol} Closed {trade['action'].upper()} position | "
                  f"Entry: ${trade['entry_price']:.4f} | "
                  f"Exit: ${current_price:.4f} | "
                  f"PNL: ${trade['pnl']:.4f} (Fee: ${trade['fee']:.4f}) | "
                  f"Reason: {reason}")

    def close_position(self, direction, reason="signal", symbol=None):
        if symbol is None:
            # Try to find any open position of this direction
            for sym in TRADING_SYMBOLS:
                data = self.symbol_data[sym]
                if direction == 'long' and data['long_position']:
                    symbol = sym
                    break
                elif direction == 'short' and data['short_position']:
                    symbol = sym
                    break
            
            if symbol is None:
                print(f"[TRADE] No open {direction.upper()} positions to close 🤷‍♂️")
                self.send_telegram_message(f"❌ No open {direction.upper()} position to close")
                return
        
        data = self.symbol_data[symbol]
        if direction == 'long' and data['long_position']:
            position = data['long_position']
        elif direction == 'short' and data['short_position']:
            position = data['short_position']
        else:
            print(f"[TRADE] {symbol} No open {direction.upper()} positions to close 🤷‍♂️")
            self.send_telegram_message(f"❌ {symbol} No open {direction.upper()} position to close")
            return
            
        current_price = self.get_current_price(symbol)
        if current_price is None:
            print(f"[TRADE] {symbol} Failed to fetch current price for closing positions")
            self.send_telegram_message(f"❌ {symbol} Failed to fetch price for closing position")
            return

        self.close_trade(position, current_price, reason)

    def close_half_position(self, direction, symbol=None):
        """Manually close half of the position"""
        if symbol is None:
            # Try to find any open position of this direction
            for sym in TRADING_SYMBOLS:
                data = self.symbol_data[sym]
                if direction == 'long' and data['long_position']:
                    symbol = sym
                    break
                elif direction == 'short' and data['short_position']:
                    symbol = sym
                    break
            
            if symbol is None:
                print(f"[HALF EXIT] No open {direction.upper()} positions to close half 🤷‍♂️")
                return
        
        data = self.symbol_data[symbol]
        if direction == 'long' and data['long_position']:
            position = data['long_position']
        elif direction == 'short' and data['short_position']:
            position = data['short_position']
        else:
            print(f"[HALF EXIT] {symbol} No open {direction.upper()} positions to close half 🤷‍♂️")
            return

        if position.get('half_exit_done', False):
            print(f"[HALF EXIT] {symbol} Half exit already executed for {direction.upper()} position")
            return

        current_price = self.get_current_price(symbol)
        if current_price is None:
            print(f"[HALF EXIT] {symbol} Failed to fetch current price")
            return

        self.execute_half_exit(position, current_price, symbol)

    def fetch_open_positions_from_api(self):
        """Fetch all open positions from Bitget API"""
        try:
            params = {
                "productType": PRODUCT_TYPE,
                "marginCoin": MARGIN_COIN
            }
            response = self.maxAccountApi.allPosition(params)
            
            if response.get('code') != '00000':
                print(f"[API POSITIONS] Error fetching positions: {response.get('msg')}")
                return []
            
            positions = response.get('data', [])
            # Filter only positions with size > 0 (open positions)
            open_positions = [pos for pos in positions if float(pos.get('total', 0)) > 0]
            
            print(f"[API POSITIONS] Fetched {len(open_positions)} open positions from API")
            return open_positions
            
        except Exception as e:
            print(f"[API POSITIONS] Error fetching positions from API: {e}")
            return []

    def close_all_positions(self):
        """Fetch all open positions from API, display them, and close all"""
        # Fetch positions from Bitget API
        api_positions = self.fetch_open_positions_from_api()
        
        if not api_positions:
            message = "📋 *No open positions found on Bitget*\n\nAll positions are already closed."
            self.send_telegram_message(message)
            print("[CLOSE ALL] No positions to close")
            return
        
        # Build message showing all open positions
        positions_msg = "📋 *Current Open Positions on Bitget:*\n\n"
        positions_to_close = []
        
        for pos in api_positions:
            symbol = pos.get('symbol', 'N/A')
            hold_side = pos.get('holdSide', '').lower()  # 'long' or 'short' (normalize to lowercase)
            total_size = float(pos.get('total', 0))
            available_size = float(pos.get('available', 0))
            avg_price = float(pos.get('averageOpenPrice', 0))
            unrealized_pnl = float(pos.get('unrealizedPL', 0))
            
            if total_size > 0 and hold_side in ['long', 'short']:
                side_emoji = "📈" if hold_side == "long" else "📉"
                positions_msg += f"{side_emoji} *{symbol} {hold_side.upper()}*\n"
                positions_msg += f"• Size: `{total_size:.4f}`\n"
                positions_msg += f"• Avg Price: `${avg_price:.4f}`\n"
                positions_msg += f"• Unrealized PNL: `${unrealized_pnl:.4f}`\n\n"
                
                positions_to_close.append({
                    'symbol': symbol,
                    'side': hold_side,
                    'size': available_size if available_size > 0 else total_size,
                    'avg_price': avg_price
                })
        
        # Send message with all positions
        self.send_telegram_message(positions_msg)
        time.sleep(1)  # Small delay before closing
        
        # Close all positions
        closed_count = 0
        failed_count = 0
        close_results = []
        
        for pos_info in positions_to_close:
            symbol = pos_info['symbol']
            side = pos_info['side']
            size = pos_info['size']
            
            # Get current price for closing
            current_price = self.get_current_price(symbol)
            if current_price is None:
                print(f"[CLOSE ALL] {symbol} Failed to get current price")
                failed_count += 1
                close_results.append(f"❌ {symbol} {side.upper()}: Failed to get price")
                continue
            
            # Determine close action
            close_action = "exit_long" if side == "long" else "exit_short"
            
            # Send close order
            order_success, order_error = self.send_order(close_action, current_price, size, symbol)
            
            if order_success:
                closed_count += 1
                # Update local position tracking if exists (mark as closed without sending another order)
                data = self.symbol_data.get(symbol)
                if data:
                    position = None
                    if side == 'long' and data.get('long_position'):
                        position = data['long_position']
                        data['long_position'] = None
                    elif side == 'short' and data.get('short_position'):
                        position = data['short_position']
                        data['short_position'] = None
                    
                    # Update position record if it exists
                    if position:
                        position['exit_time'] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                        position['exit_price'] = current_price
                        if position['action'] == 'long':
                            position['pnl'] = (current_price - position['entry_price']) * position['size']
                        else:
                            position['pnl'] = (position['entry_price'] - current_price) * position['size']
                        position['fee'] = (current_price + position['entry_price']) * FEE_PERCENT * position['size']
                        position['ideal_pnl'] = position['pnl']
                        position['pnl'] -= position['fee']
                        self.balance = self.fetch_real_balance()
                        position['status'] = 'closed'
                        position['reason'] = 'close_all_manual'
                        self.save_trades()
                
                close_results.append(f"✅ {symbol} {side.upper()}: Closed at ${current_price:.4f}")
                print(f"[CLOSE ALL] ✅ Closed {symbol} {side.upper()} position")
            else:
                failed_count += 1
                close_results.append(f"❌ {symbol} {side.upper()}: {order_error}")
                print(f"[CLOSE ALL] ❌ Failed to close {symbol} {side.upper()}: {order_error}")
            
            time.sleep(0.5)  # Small delay between closes
        
        # Send final summary message
        summary_msg = f"🛑 *CLOSE ALL POSITIONS - COMPLETE*\n\n"
        summary_msg += f"✅ Successfully Closed: `{closed_count}`\n"
        summary_msg += f"❌ Failed: `{failed_count}`\n\n"
        summary_msg += "*Results:*\n"
        for result in close_results:
            summary_msg += f"{result}\n"
        
        self.send_telegram_message(summary_msg)
        print(f"[CLOSE ALL] Completed: {closed_count} closed, {failed_count} failed")

    # ========== Risk Management ==========
    def monitor_positions(self):
        print("[MONITOR] Starting real-time position monitoring (every 1s)...")
        while self.running:
            try:
                # Check all symbols
                for symbol in TRADING_SYMBOLS:
                    current_price = self.get_current_price(symbol)
                    if current_price:
                        self.check_risk(current_price, symbol)
                    else:
                        print(f"[MONITOR] {symbol} Skipping — price unavailable")
                
                # Sleep exactly 1 second before next check
                time.sleep(1)

            except Exception as e:
                print(f"[MONITOR] Error: {e}")
                time.sleep(1)

    def check_risk(self, current_price, symbol):
        data = self.symbol_data[symbol]
        
        if data['long_position']:
            self.update_trailing_stop(current_price, data['long_position'])
        if data['short_position']:
            self.update_trailing_stop(current_price, data['short_position'])

        # Check long position
        if data['long_position']:
            position = data['long_position']
            if current_price <= position['stop_loss']:
                print(f"[RISK] {symbol} Stop loss hit for LONG: ${current_price:.4f} <= ${position['stop_loss']:.4f}")
                self.send_telegram_message(
                    f"🛑 *STOP LOSS HIT - {symbol} LONG*\n"
                    f"Price: `${current_price:.4f}`\n"
                    f"Stop Loss: `${position['stop_loss']:.4f}`")
                self.close_trade(position, current_price, "stop_loss")
                return

        # Check short position  
        if data['short_position']:
            position = data['short_position']
            if current_price >= position['stop_loss']:
                print(f"[RISK] {symbol} Stop loss hit for SHORT: ${current_price:.4f} >= ${position['stop_loss']:.4f}")
                self.send_telegram_message(
                    f"🛑 *STOP LOSS HIT - {symbol} SHORT*\n"
                    f"Price: `${current_price:.4f}`\n"
                    f"Stop Loss: `${position['stop_loss']:.4f}`")
                self.close_trade(position, current_price, "stop_loss")
                return

    # ========== Utility Methods ==========
    def send_order(self, action, price, size=None, symbol=None):
        """Send order to exchange and return (success: bool, error_msg: str)"""
        if symbol is None:
            return
        if action in ["short", "long"]:
            side = "sell" if action == "short" else "buy"
            params = {
                "symbol": symbol,
                "productType": PRODUCT_TYPE,
                "marginMode" : MARGIN_MODE,
                "marginCoin": MARGIN_COIN,
                "size": str(round(size, 2)) if size else "",
                "side": side,
                "tradeSide": "open",
                "orderType": self.orderType,
                "force": "gtc",
                "price": str(round(price, 4)),
            }

            self.total_trades += 1

        elif action in ["exit_short", "exit_long"]:
            side = "buy" if action == "exit_long" else "sell"
            params = {
                "symbol": symbol,
                "productType": PRODUCT_TYPE,
                "marginMode" : MARGIN_MODE,
                "marginCoin": MARGIN_COIN,
                "size": str(round(size, 2)) if size else "",
                "side": side,
                "tradeSide": "close",
                "orderType": self.orderType,
                "force": "gtc",
                "price": str(round(price, 4))
            }
            
        else:
            return False, "Invalid action"
        
        print(f"[Signal 🟡] {symbol} Sending: {params}")

        if self.flag_api_sent:
            try:
                response = self.maxOrderApi.placeOrder(params)
                result = response
                if result.get('code') == '00000':
                    print(f"[Signal 🟢] {symbol} Order placed successfully: {result.get('data')}")
                    return True, None
                else:
                    error_msg = result.get('msg', 'Unknown error')
                    print(f"[Signal 🔴] {symbol} Order placement failed: {error_msg}")
                    return False, error_msg
            except Exception as e:
                error_msg = f"Exception: {str(e)}"
                print(f"[Signal 🔴] {symbol} Failed order: {error_msg}")
                return False, error_msg
        else:
            print(f"[Signal 🟡] {symbol} Order sending is disabled. Order not sent.")
            return False, "Order sending is disabled"

    def save_trades(self):
        try:
            with open(CSV_FILE, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['symbol', 'entry_time', 'exit_time', 'action', 'entry_price', 'exit_price', 
                               'size', 'status', 'fee', 'ideal_pnl', 'pnl', 'reason', 'stop_loss', 
                               'take_profit', 'max_profit_price', 'trailing_stop_active', 'half_exit_done', 'original_size'])
                for trade in self.trades:
                    writer.writerow([
                        trade.get('symbol'),
                        trade['entry_time'],
                        trade['exit_time'],
                        trade['action'],
                        round(trade['entry_price'], 4),
                        round(trade['exit_price'], 4) if trade['exit_price'] else '',
                        round(trade['size'], 2),
                        trade['status'],
                        round(trade['fee'], 4),
                        round(trade['ideal_pnl'], 4),
                        round(trade.get('pnl', 0), 4),
                        trade['reason'],
                        round(trade['stop_loss'], 4),
                        round(trade['take_profit'], 4),
                        round(trade.get('max_profit_price', 0), 4),
                        str(trade.get('trailing_stop_active', False)),
                        str(trade.get('half_exit_done', False)),
                        round(trade.get('original_size', trade['size']), 2)
                    ])
            print(f"[DATA] Saved {len(self.trades)} trades to {CSV_FILE}")
        except Exception as e:
            print(f"[DATA] Error saving trades: {e}")

    def wait_until_next_hour_plus_5sec(self):
        now = datetime.now(timezone.utc)
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        next_time = current_hour + timedelta(hours=1, seconds=5)

        if next_time <= now:
            next_time += timedelta(hours=1)

        sleep_seconds = (next_time - now).total_seconds()
        print(f"Sleeping for {sleep_seconds:.4f} seconds until {next_time.strftime('%H:%M:%S')} UTC")
        time.sleep(sleep_seconds)

    # ========== Main Loop ==========
    def strategy_loop(self):
        print("[STRATEGY] Starting strategy loop")
        while self.running:
            try:
                current_time = datetime.now(timezone.utc)
                print(f"\n[STRATEGY] Running strategy check at {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                self.update_indicators()
                self.check_signal(current_time.strftime('%Y-%m-%d %H:%M:%S'))
                self.wait_until_next_hour_plus_5sec()
            except Exception as e:
                print(f"[STRATEGY] Error in strategy loop: {e}")
                time.sleep(60)

def main():
    trading_bot = AutoTradeBot()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    trading_bot.set_telegram_app(application)
    
    application.add_handler(CommandHandler("start", trading_bot.handle_start))
    application.add_handler(CommandHandler("help", trading_bot.handle_help))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, trading_bot.handle_message))
    application.add_handler(CallbackQueryHandler(trading_bot.handle_callback))
    
    print("🤖 Starting Trading Bot with Telegram control...")
    print("✅ Trading strategies are running in background")
    print("✅ Telegram bot is ready for commands")
    print(f"✅ Bot is restricted to authorized chat ID: {AUTHORIZED_CHAT_ID}")
    print("✅ Send /start to your bot to begin")
    
    application.run_polling()

if __name__ == "__main__":
    main()